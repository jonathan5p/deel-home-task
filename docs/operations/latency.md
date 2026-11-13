# End-to-end latency

`make latency-probe` measures how long a source commit takes to reach landing, bronze, silver and PostgreSQL gold, per entity, on the running pipeline. It makes one traceable change per entity, polls every layer until it holds the change, and reports the latency per layer, the time per hop and percentiles over several probes.

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

The target first checks that the four jobs and `iceberg-rest` run, and exits 2 naming what is missing. The probe is plain Python that reads every layer with DuckDB, so it takes no cluster cores. Only the probe's container gets the source database login.

## What it changes in the source

Each probe runs one source transaction that sets `updated_by` on one row of each entity (the lowest key unless `KEYS` says otherwise) to a marker unique to the probe, the negative epoch milliseconds of its start. The source's own jobs write `-1`, so the marker is easy to tell apart.

`updated_by` is an audit column, left out of the content hash, so the change is a folded version: bronze and silver append it, gold stores it folded, and no public view or business metric changes. The probe writes to the source, so run it on local stacks only.

## How it measures

| Layer | Visible when | Read with |
| --- | --- | --- |
| Landing | a file the topic's file sink committed after the probe started holds the change | the sink log `_spark_metadata/` (the latest `.compact` log and the batch logs after it), then `read_avro` of only the new files |
| Bronze | `lake.bronze.<entity>` holds the change's key and `_commit_sequence` | the Iceberg REST catalog, filtered on the `_published_at` partition |
| Silver | `lake.silver.<entity>` holds them | the Iceberg REST catalog, filtered on the `_changed_at` partition |
| Gold | the entity's `gold_internal` dimension or history fact holds them | a query run in the analytics database (`postgres_query`), as the gold owner |

Landing finds the change by topic, key and marker; later layers by key and `_commit_sequence`. The lake gold views are not polled: Iceberg commits before the PostgreSQL copy, so they are never behind PostgreSQL gold.

- **Latency** of a layer: from the source commit (`source.ts_ms`) to the first poll that saw the change, so at most one `POLL_SECONDS` above the exact moment.
- **Hops** split the latency to gold: `commit_to_debezium` (envelope `ts_ms` minus `source.ts_ms`), `debezium_to_kafka` (the Kafka timestamp minus `ts_ms`), `kafka_to_landing`, `landing_to_bronze`, `bronze_to_silver` and `silver_to_gold` (each from one layer's first sighting to the next one's).
- **Percentiles** (min, p50, p90, p95, max) are per entity and layer, nearest rank over the probes that reached the layer.
- **Probes** run one after another, each after a random wait of up to the longest trigger, so they land at different points of the jobs' cycles.

## Reading the numbers against the trigger intervals

Each job starts a batch every `*_TRIGGER_SECONDS` (landing 10, bronze and silver 2, gold 5 in `.env`; [configuration](configuration.md)). A change:

1. reaches Kafka within about a second of its commit;
2. waits 0 to 10 seconds for landing's next batch, plus the batch itself;
3. waits for the next batch of each later job: between 0 and that job's own interval, plus its batch. With equal intervals the wait is a full interval, because both jobs start on the same boundaries.

So expect landing 1–12 s after the commit and gold about 5–8 s after landing, plus gold's own batch, which can outlast its 5-second trigger ([sizing](../architecture/sizing.md#gold)). Measured on 2026-11-13: gold 13 s at p50 and 16 s at p95 for customers and products, 16 s and 19–20 s for orders and order items. With 10-second triggers everywhere, gold was about 39 s after the commit ([sizing](../architecture/sizing.md#measured-on-the-real-load)). A hop well above its interval means a batch outlasted the interval, or the job was restarting.

## Output and exit codes

The probe prints one line per probe, a percentile table, the hop medians, a `TIMEOUT` or `THRESHOLD` line per failing probe, and a final `LATENCY` summary with the report paths.

| Exit code | When |
| --- | --- |
| 0 | every probe reached gold, within every `MAX_LATENCY` limit |
| 1 | a probe timed out, or a probe was slower than a limit |
| 2 | the probe could not run: a missing setting, option or DuckDB extension, a job or the catalog not running, an empty source table, or a read that failed |

Through `make`, which always exits 2 on failure, tell them apart by its `Error 1` or `Error 2` message.

## Reports

Every run writes `<LAKE_ROOT>/latency/reports/<UTC>.json` and a static `<UTC>.html` page, and rebuilds `index.html`, the list of runs. Open it at <http://localhost:8888/buckets/lake/latency/reports/index.html>.

## Latest results

The [latest latency results](latency-results.md) page shows one run, rendered on every docs build from `docs/results/latency/latest.json` (with its [HTML page](../results/latency/latest.html)).

```sh
make latency-probe     # on the running pipeline: writes a new report to the lake
make docs-results      # copy the newest reports from the lake into docs/results/, then commit them
```

`make docs-results` reads the lake through the SeaweedFS filer (`LAKE_FILER_URL`, default <http://localhost:8888>). It copies the newest run that passed, and the page names any newer failed runs; `RESULTS=latency` copies only this report. It exits 2 when the filer cannot be reached and 1 when a report is missing.
