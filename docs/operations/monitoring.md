# Monitoring Spark

The `metrics` profile, on by default, scrapes every Spark driver, the master and the workers into [Prometheus](https://prometheus.io/), and [Grafana](../serving/grafana.md) charts them. It answers two questions: do the streams keep up, and how much of the cluster do the jobs use ([key metrics](../architecture/sizing.md#key-metrics)).

## Start it

```sh
make up                        # Prometheus on http://localhost:9090, the Spark History Server, the REST catalog and the exporter
make pipeline PIPELINE=all     # Then the streaming jobs, if they are not running yet
```

Grafana at <http://localhost:3000> has the **Spark cluster**, **Spark streaming**, **Spark stages** and **Iceberg tables** dashboards in the **Spark and lake** folder. Prometheus scrapes every 10 seconds and keeps 30 days (or 2 GB) in the `prometheus-data` volume, which survives `make down`.

## What is scraped

Spark's [`PrometheusServlet`](https://spark.apache.org/docs/latest/monitoring.html#metrics) serves the metrics on each process's UI port. Nothing is pushed.

| Job | Targets | Path | Holds |
| --- | --- | --- | --- |
| `spark-drivers` | every running driver, port 4040 | `/metrics/prometheus` | Streaming query rates, batch duration and state (`spark.sql.streaming.metricsEnabled`), stage and task counters, driver JVM |
| `spark-executors` | every running driver, port 4040 | `/metrics/executors/prometheus` | Per executor: active and failed tasks, task and GC time, shuffle bytes, peak heap and process memory (`spark.executor.processTreeMetrics.enabled`) |
| `spark-master` | `spark-master:8080` | `/metrics/master/prometheus` | Alive workers, running and waiting applications |
| `spark-applications` | `spark-master:8080` | `/metrics/applications/prometheus` | Cores granted to each application |
| `spark-workers` | `spark-worker-1:8081`, `spark-worker-2:8081` | `/metrics/prometheus` | Cores and memory used and free per worker |
| `spark-stages` | `spark-stage-exporter:9101` | `/metrics` | Completed stages of every application, from the History Server (see [Stage history](#stage-history)), and the lake's Iceberg table statistics (see [Iceberg tables](#iceberg-tables)) |

- Drivers are found by their Compose service name through Docker's DNS, re-resolved every 10 seconds; the `service` label names the job. `make spark-submit` drivers answer to `spark-app`.
- `monitoring/prometheus/prometheus.yml` rewrites metric names so they stay the same across runs: the application id leaves the names, a streaming query's name becomes the `query` label of `spark_streaming_<measure>`, and only the `_Value` copy of each gauge is kept.

## Stage history

Stage and task details (duration, task skew, shuffle, spill) live in the event logs. The `spark-stage-exporter` service reads them from the History Server's REST API every 30 seconds and serves them on port 9101. It reads each completed stage's task quantiles once, and counts only stages that finish after it starts. Series are labelled with the application (`app`) and the streaming query (`query`) only, so their number stays bounded.

| Metric | Type | From the stage |
| --- | --- | --- |
| `spark_stage_run_seconds_total`, `spark_stage_cpu_seconds_total`, `spark_stage_gc_seconds_total` | counter | Task run, CPU and GC time |
| `spark_stage_input_bytes_total`, `spark_stage_input_records_total`, `spark_stage_output_bytes_total`, `spark_stage_output_records_total` | counter | Bytes and records read from sources and written to sinks |
| `spark_stage_shuffle_read_local_bytes_total`, `spark_stage_shuffle_read_remote_bytes_total`, `spark_stage_shuffle_write_bytes_total` | counter | Shuffle bytes read from the same executor, read from other executors, and written |
| `spark_stage_shuffle_fetch_wait_seconds_total` | counter | Time tasks waited for shuffle blocks |
| `spark_stage_memory_spill_bytes_total`, `spark_stage_disk_spill_bytes_total` | counter | Bytes spilled from memory and to disk |
| `spark_stage_duration_seconds` | histogram | Submission to completion; its `_count` counts the stages |
| `spark_stage_tasks` | histogram | Tasks per stage |
| `spark_stage_median_task_seconds` | histogram | Duration of the median task |
| `spark_stage_task_duration_skew` | histogram | Slowest task duration over the median task duration |
| `spark_stage_data_skew` | histogram | Largest task read over the median task read, in shuffle bytes for stages that read a shuffle and in input bytes otherwise; left out when the median task reads nothing |

The **Spark stages** dashboard charts them.

## Iceberg tables

The same exporter measures the lake's Iceberg tables with [PyIceberg](https://py.iceberg.apache.org/) through the read-only REST catalog: every 5 minutes the data files of each table in `bronze`, `silver` and `gold_internal` (count, bytes, records, average size, files under 32 MiB), and every hour the partitions that hold small files.

| Metric | Labels | Holds |
| --- | --- | --- |
| `iceberg_table_data_files` | `namespace`, `table` | Data files in the current snapshot |
| `iceberg_table_data_bytes`, `iceberg_table_records` | `namespace`, `table` | Their bytes and records |
| `iceberg_table_average_file_bytes` | `namespace`, `table` | Their average size |
| `iceberg_table_small_files` | `namespace`, `table` | Those below `SMALL_FILE_BYTES` |
| `iceberg_partition_small_files` | `namespace`, `table`, `partition` | Small files per partition that has any; a partition compaction fixed disappears |

Every batch adds files and [lake maintenance](maintenance-and-recovery.md#lake-maintenance) merges them, so the files per table form a saw-tooth. A line that only climbs means maintenance is not running.

## Gold batch log

The gold job logs one line per batch of each stream, `GOLD BATCH ` followed by a JSON object:

```text
GOLD BATCH {"load":"fact_order_history","batch_id":<id>,"seconds":<s>,"jobs":<n>,"written":{"fact_order_history":<rows>,"current_orders":<rows>},"steps":[{"step":"catch-up","seconds":<s>,"jobs":<n>},{"step":"compute fact_order_history",...},{"step":"merge fact_order_history",...},{"step":"compute current_orders",...},{"step":"merge current_orders",...},{"step":"snapshots",...},{"step":"copy",...}]}
```

| Field | Holds |
| --- | --- |
| `load` | The stream: `dim_customer`, `dim_product`, `fact_order_history` or `fact_order_item_history` |
| `batch_id` | The micro-batch id of the stream's checkpoint |
| `seconds`, `jobs` | The batch's wall time and its own Spark jobs (other streams' jobs are not counted) |
| `written` | The rows computed for each table; a table with 0 gets no merge and no copy |
| `steps` | Each step with its seconds and Spark jobs: `catch-up`, `compute <table>`, `merge <table>`, `snapshots` and `copy` (JDBC staging and the PostgreSQL transaction) |

```sh
docker compose logs gold-job | grep 'GOLD BATCH'                                                   # every batch
docker compose logs --no-log-prefix gold-job | sed -n 's/.*GOLD BATCH //p' | jq -c '{load, seconds, jobs}'   # the totals, one line per batch
```

A batch replayed after a commit conflict logs once, with every attempt's steps. Measured values are in [sizing](../architecture/sizing.md#the-numbers-that-matter).

## Alerts

`monitoring/prometheus/rules/spark.yml` defines two alerts, each firing after 5 minutes. It also records each job's trigger as `spark_streaming_trigger_seconds`; a unit test fails when it disagrees with `.env`, so change both together.

| Alert | Fires when |
| --- | --- |
| `StreamingQueryFallingBehind` | A query's processing rate stays below its input rate: its backlog grows |
| `StreamingBatchOverTrigger` | A query's micro-batches take longer than its job's trigger interval (`spark_streaming_trigger_seconds`): batches start late |

They show at <http://localhost:9090/alerts>. No Alertmanager runs, so nobody is notified. `tests/prometheus/spark_rules_test.yml` holds their `promtool` tests.

## Read the panels for sizing

| Question | Dashboard → panel | Look for |
| --- | --- | --- |
| Is the cluster full? | Spark cluster → Worker cores used and free, Applications running and waiting | Free cores near zero while applications wait |
| Do the jobs use the cores they hold? | Spark cluster → Busy task slots per application | Busy slots well under the granted cores mean `spark.cores.max` can shrink |
| Is the executor memory enough? | Spark cluster → Executor peak heap and RSS vs 1 GiB, GC share of task time | Heap peaks near 1 GiB or GC above 10 % mean more memory or smaller partitions |
| Does each query keep up? | Spark streaming → Input vs processing rate, Processing / input ratio | Ratio at or above 1 |
| Is the trigger right? | Spark streaming → Batch duration / trigger, Share of time over the trigger | Batches well under 100 % of their job's trigger; a growing share means a longer trigger or more cores |
| Does silver's state level off? | Spark streaming → State rows, State size, State growth | Growth falling towards zero once the deduplication window is full |
| Are the partitions the right size? | Spark stages → Tasks per stage, Stages with a median task under 100 ms, Stage duration p50 and p95 | Many stages with sub-100 ms median tasks mean fewer partitions (`spark.sql.shuffle.partitions`) would do |
| Is the work even across tasks? | Spark stages → Skew p95 | Skews above about 3 mean a hot key or uneven input files |
| Is memory or the shuffle the bottleneck? | Spark stages → GC time / run time, Spill, Shuffle fetch wait / run time | Any spill, GC above 10 % or a growing fetch wait share |
| Does compaction keep up? | Iceberg tables → Data files per table, Data files added or removed per hour, Small files per table, Partitions with the most small files | Files dropping at each maintenance run; small files and their partitions shrinking back |
| How much data does each job move? | Spark stages → Bytes read and written, Records read and written, Bytes per record read | The volumes to project the cluster size from |

Use a time range that covers a burst of source changes and the quiet after it: the rates and durations are those of the last micro-batch, sampled every 10 seconds.

## Network-proxy limits

Spark has no network metric. Its closest signals are the shuffle fetch wait (`spark_stage_shuffle_fetch_wait_seconds_total`) and remote shuffle reads (`spark_stage_shuffle_read_remote_bytes_total`). Locally both understate the network: the two workers share one Docker bridge, so a remote read is a memory copy, and with two workers about half the shuffle is remote whatever the job does. Use them to compare runs on the same machine, not to forecast a real network.
