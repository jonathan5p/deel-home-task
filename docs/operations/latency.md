# End-to-end latency

`make latency-probe` measures how long a change committed in the source PostgreSQL takes to become visible in landing, bronze, silver and PostgreSQL gold, per entity, against the running pipeline. It makes one traceable change per entity, polls every layer until each one holds it, and reports the latency from the source commit to each layer, the time each hop took, and percentiles over several probes. Like the [data quality checks](../lakehouse/data-quality.md), it runs on demand against the shared stack, never inside `make test-infra`.

```sh
make up PROFILES=catalog && make pipeline PIPELINE=all   # the probe needs the four jobs and the REST catalog
make latency-probe                                       # 5 probes of every entity
make latency-probe ENTITIES=customers,orders PROBES=3
make latency-probe MAX_LATENCY=gold=60,silver=40         # fail when a probe is slower
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ENTITIES` | every entity | Comma-separated entities to probe: `customers`, `products`, `orders`, `order_items` |
| `PROBES` | `5` | Probes to run, one after the other |
| `POLL_SECONDS` | `1` | Seconds between two polls of the layers a probe has not reached yet |
| `TIMEOUT_SECONDS` | `300` | Seconds a probe waits for gold before it gives up |
| `MAX_LATENCY` | none | Limits in seconds per layer, as `<layer>=<seconds>` pairs, such as `gold=60,silver=40` |
| `KEYS` | the lowest key | The row each entity's probes mark, as `<entity>=<key>` pairs, such as `customers=7` |

The target first checks that the four pipeline jobs and `iceberg-rest` are running; if one is not, it names it, prints how to start the stack (`make up PROFILES=catalog && make pipeline PIPELINE=all`) and exits 2 without starting anything. The probe itself is plain Python in the `spark-app` image (`python -m pipelines.latency.job`), not a Spark application: it reads every layer with DuckDB, so it takes no cores from the cluster. The target passes the probe's own variables from `.env` (the REST catalog and S3 settings and the source database login, see [configuration](configuration.md)) to the probe's container only, so the pipeline jobs and the other drivers never get the source database login.

## What it changes in the source

Each probe runs one transaction in the source database that sets `updated_by` on one existing row of each probed entity to a marker unique to that probe: the negative epoch milliseconds of the probe's start, such as `-1794426435487`. The source's own scheduled jobs write `-1`, so a probe's change is easy to tell apart, and the row is the lowest key of each table unless `KEYS` names another. Every entity of a probe shares that one commit, so their latencies start from the same moment.

`updated_by` is an audit column, and the content hash that bronze computes leaves the key and the audit columns out, so the change carries the same `_content_hash` as the version before it:

- **Lake:** bronze and silver append the change like any other (one row per probed entity and probe), and the silver `_history` and `_current` views and the lake gold views fold it into the version it repeats.
- **PostgreSQL gold:** the dimensions and history facts append it to `gold_internal` as a folded version (one row per probed entity and probe, so about four per probe), which the public `gold` views leave out; the as-of lookups of the facts skip folded versions too. No business metric, dimension version or fact row of the `gold` views changes.

The probe writes to the source database, so run it on local stacks only.

## How it measures

| Layer | Visible when | Read with |
| --- | --- | --- |
| Landing | a file the topic's file sink committed after the probe started holds the change | the sink log `_spark_metadata/` (the latest `.compact` log and the batch logs after it), then `read_avro` of only the new files |
| Bronze | `lake.bronze.<entity>` holds the change's key and `_commit_sequence` | the Iceberg REST catalog, filtered on the `_published_at` partition |
| Silver | `lake.silver.<entity>` holds them | the Iceberg REST catalog, filtered on the `_changed_at` partition |
| Gold | the entity's `gold_internal` dimension or history fact holds them | a query run in the analytics database (`postgres_query`), as the gold owner |

Landing identifies the change by its topic, key and marker, and gives its `_commit_sequence` (Debezium `source.lsn`), the source commit time (`source.ts_ms`), the time Debezium read it (envelope `ts_ms`) and the Kafka record timestamp. The later layers are matched on the key and `_commit_sequence`. Gold counts as visible once `gold_internal` holds the version, whether folded or not, because that is when the gold job has written it. The lake gold views are not polled: they are computed from silver at read time, so they are as fresh as silver.

- **Latency** of a layer is the time from the source commit (`source.ts_ms`) to the first poll that saw the change there. A poll's time is when it started, so a latency is at most one `POLL_SECONDS` plus one lookup above the exact moment.
- **Hops** split the latency to gold: `commit_to_debezium` (envelope `ts_ms` minus `source.ts_ms`), `debezium_to_kafka` (the Kafka timestamp minus `ts_ms`), `kafka_to_landing`, `landing_to_bronze`, `bronze_to_silver` and `silver_to_gold` (each from one layer's first sighting to the next one's).
- **Percentiles** (min, p50, p90, p95, max) are per entity and layer, nearest rank over the probes that reached the layer.
- **Probes** run one after another, each after a random wait of up to the longest trigger interval, so they land at different points of the jobs' cycles. Polls check the layers from gold back to landing, so a poll never sees a later layer before an earlier one.

## Reading the numbers against the trigger intervals

Every job runs a micro-batch every `*_TRIGGER_SECONDS` (10 by default, see [configuration](configuration.md)), and Spark starts those batches on multiples of the interval, so jobs with the same interval start their batches at the same moments. A change therefore:

1. reaches Kafka within about a second of its commit (`commit_to_debezium` plus `debezium_to_kafka`);
2. waits in Kafka for landing's next batch, anywhere between 0 and `LANDING_TRIGGER_SECONDS`, then for that batch to finish (`kafka_to_landing`);
3. waits about one full interval at each later hop, because the next job started its batch at the same moment as the job before it, before that batch committed the change, and picks it up one interval later, plus the difference between the two batches' durations.

With the default 10-second triggers, expect landing between about 1 and 12 seconds after the commit, then about 10 seconds more for each of bronze, silver and gold, so gold about 30 seconds after landing. With different intervals, a hop waits for the next batch of the downstream job instead, anywhere between 0 and its interval. A hop well above its job's interval means a batch took longer than the interval (Spark then starts the next one late), or the job was stopped or restarting.

## Output and exit codes

The probe prints one line per probe with each entity's latency per layer, then a table of the percentiles per entity and layer, the median of each hop, one `TIMEOUT` line per probe that never reached gold (with the last layer it reached) and one `THRESHOLD` line per probe slower than a limit. The last line is a `LATENCY` summary, like the `DATA QUALITY` one: whether the run passed, the number of probes, timeouts and exceeded limits, the p95 per entity and layer, and the paths of the reports.

| Exit code | When |
| --- | --- |
| 0 | every probe reached gold, within every `MAX_LATENCY` limit |
| 1 | a probe timed out, or a probe was slower than a limit |
| 2 | the probe could not run: a missing setting or option, a job or the catalog not running, an empty source table, or a read that failed |

These are the exit codes of the probe itself (`python -m pipelines.latency.job`). `make` exits 2 whenever a recipe fails, so through `make latency-probe` tell them apart by its `Error 1` or `Error 2` message.

## Reports

Every run writes two files to the lake's object storage, named after the run's start in UTC, like the data quality reports:

- `<LAKE_ROOT>/latency/reports/<UTC>.json`: the run's settings (entities, probes, poll interval, timeout, each job's trigger interval, limits, the probed keys), its summary (pass or fail, timeouts, exceeded limits, the percentiles of every layer and hop) and every probe's change with when each layer saw it.
- `<LAKE_ROOT>/latency/reports/<UTC>.html`: the same as a static page: the summary and settings, the percentile table with each limit's status, a chart of the median latency per entity and layer, the hop breakdown and one row per probe. It has no scripts or external assets.

Each run also rebuilds `<LAKE_ROOT>/latency/reports/index.html`, the list of every run, newest first, with its status and links to both files. SeaweedFS serves the lake bucket over HTTP, with HTML as `text/html`, so the reports open in a browser at <http://localhost:8888/buckets/lake/latency/reports/index.html>, and `curl http://localhost:8888/buckets/lake/latency/reports/` lists the files.
