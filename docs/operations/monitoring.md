# Monitoring Spark

The `metrics` profile, which `make up` starts by default, scrapes every Spark driver, the master and the workers into [Prometheus](https://prometheus.io/), and [Grafana](../serving/grafana.md) charts them. It answers two questions: are the streaming queries keeping up right now, and how much of the cluster do the jobs really use, to size it (see [the sizing strategy](../architecture/sizing.md#key-metrics)).

## Start it

```sh
make up                        # Prometheus on http://localhost:9090, the Spark History Server, the REST catalog and the exporter
make pipeline PIPELINE=all     # Then the streaming jobs, if they are not running yet
```

Then open Grafana at <http://localhost:3000>: the **Spark and lake** folder holds the **Spark cluster**, **Spark streaming**, **Spark stages** and **Iceberg tables** dashboards, and the links bar at the top of each one moves between them. The **Gold metrics** dashboard stays in the **Gold** folder.

Prometheus scrapes every 10 seconds and keeps 30 days of samples, or 2 GB, whichever comes first, in the `prometheus-data` volume, so the history survives `make down`, a restart of Prometheus and restarts of the drivers. `make clean CONFIRM=1` deletes it. Without the profile, Grafana's Prometheus datasource reports an error and the Spark dashboards stay empty; the Gold dashboard is not affected.

## What is scraped

Spark's own [`PrometheusServlet`](https://spark.apache.org/docs/latest/monitoring.html#metrics) serves the metrics on each process's web UI port, configured in `docker/spark/metrics.properties`. Nothing is pushed: a driver that is not scraped pays nothing.

| Job | Targets | Path | Holds |
| --- | --- | --- | --- |
| `spark-drivers` | every running driver, port 4040 | `/metrics/prometheus` | Streaming query rates, batch duration and state (`spark.sql.streaming.metricsEnabled`), stage and task counters, driver JVM |
| `spark-executors` | every running driver, port 4040 | `/metrics/executors/prometheus` | Per executor: active and failed tasks, task and GC time, shuffle bytes, peak heap and process memory (`spark.executor.processTreeMetrics.enabled`) |
| `spark-master` | `spark-master:8080` | `/metrics/master/prometheus` | Alive workers, running and waiting applications |
| `spark-applications` | `spark-master:8080` | `/metrics/applications/prometheus` | Cores granted to each application |
| `spark-workers` | `spark-worker-1:8081`, `spark-worker-2:8081` | `/metrics/prometheus` | Cores and memory used and free per worker |
| `spark-stages` | `spark-stage-exporter:9101` | `/metrics` | Completed stages of every application, from the History Server (see [Stage history](#stage-history)), and the lake's Iceberg table statistics (see [Iceberg tables](#iceberg-tables)) |

Drivers are found by their Compose service name. Docker's DNS resolves `landing-job`, `bronze-job`, `silver-job`, `gold-job`, `lake-views`, `maintenance-job`, `data-quality-job` and `spark-app` to the containers running them, and Prometheus asks again every 10 seconds, so a driver is scraped from its first 10 seconds until it ends; the `service` label names it. The drivers started by `make spark-submit`, `make spark-sql` and the targets built on them join the network with the `spark-app` alias (`docker compose run --use-aliases`), so they are scraped too, under `service="spark-app"`.

The configuration in `monitoring/prometheus/prometheus.yml` also reshapes the metric names, so that they stay the same from one run to the next:

- `spark.metrics.namespace=spark` replaces the application id in the driver metric names (`metrics_spark_driver_...`), which would otherwise start a new series at every restart.
- A streaming query's metrics carry the query name in the metric name; they become `spark_streaming_<measure>{query="<query>"}` (`inputRate_total`, `processingRate_total`, `latency`, `states_rowsTotal`, `states_usedBytes`, `eventTime_watermark`), with the query name's spaces and dots turned into `_`.
- The applications page carries the application name and start time in the metric name; it becomes `spark_application_cores{app="<name>"}`.
- Executor metrics name the application in an `app` label, without the application id.
- Spark repeats every gauge as `_Number` and `_Value`; only `_Value` is kept, and the code generation histograms and listener timers are dropped.

## Stage history

Spark's own metrics stop at counters per driver; the stage and task details that size a cluster (how long each stage took, how even its tasks were, how much it shuffled and spilled) live in the event logs. The `spark-stage-exporter` service of the `metrics` profile reads them from the History Server's REST API and serves them to Prometheus on its internal port 9101 (`python -m pipelines.observability.stage_exporter`, in the `spark-app` image):

- every 30 seconds (`STAGE_EXPORTER_POLL_SECONDS`) it lists the applications that run or ended since it started, and their completed stages;
- for each stage it has not seen yet, it reads the task quantiles once (`taskSummary?quantiles=0,0.5,0.95,1`) and adds the stage to its counters and histograms;
- only stages that finish after it starts are counted, so a restart never counts a stage twice.

Every series is labelled with the application name (`app`) and the streaming query (`query`, the first line of the stage's description, which Spark sets to the query name for every micro-batch; empty for batch jobs, and so absent in Prometheus). Nothing finer, such as stage ids, becomes a label, so the number of series stays bounded.

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

The **Spark stages** dashboard charts them; the questions it answers are in the table below.

## Iceberg tables

The same exporter process (`python -m pipelines.observability.exporter`) also measures the lake's Iceberg tables with [PyIceberg](https://py.iceberg.apache.org/), through the read-only REST catalog (`iceberg-rest`, which the `metrics` profile starts too) and the S3 files it points at:

- every 5 minutes (`TABLE_STATS_INTERVAL_SECONDS`), for every table of the namespaces in `TABLE_STATS_NAMESPACES` (default `bronze,silver,gold_internal`, so the gold Iceberg tables are measured too), the data files of its current snapshot: how many, their bytes and records, their average size and how many are below `SMALL_FILE_BYTES` (default 32 MiB);
- every hour (`PARTITION_STATS_INTERVAL_SECONDS`), the same tables per partition, keeping only the partitions that hold small files.

| Metric | Labels | Holds |
| --- | --- | --- |
| `iceberg_table_data_files` | `namespace`, `table` | Data files in the current snapshot |
| `iceberg_table_data_bytes`, `iceberg_table_records` | `namespace`, `table` | Their bytes and records |
| `iceberg_table_average_file_bytes` | `namespace`, `table` | Their average size |
| `iceberg_table_small_files` | `namespace`, `table` | Those below `SMALL_FILE_BYTES` |
| `iceberg_partition_small_files` | `namespace`, `table`, `partition` | Small files per partition that has any; a partition compaction fixed disappears |

Every micro-batch adds files, and [lake maintenance](maintenance-and-recovery.md#lake-maintenance) merges them, so the **Iceberg tables** dashboard shows the data files per table as a saw-tooth and the hourly change in files (`delta(iceberg_table_data_files[1h])`, a gauge) dropping at each compaction. A line that only climbs means maintenance is not running.

## Gold batch log

The gold job logs one line per micro-batch of each of its four streams, to the driver's standard error, prefixed with `GOLD BATCH ` and followed by one JSON object:

```text
GOLD BATCH {"load":"fact_order_history","batch_id":<id>,"seconds":<s>,"jobs":<n>,"written":{"fact_order_history":<rows>,"current_orders":<rows>},"steps":[{"step":"catch-up","seconds":<s>,"jobs":<n>},{"step":"compute fact_order_history",...},{"step":"merge fact_order_history",...},{"step":"compute current_orders",...},{"step":"merge current_orders",...},{"step":"snapshots",...},{"step":"copy",...}]}
```

| Field | Holds |
| --- | --- |
| `load` | The stream: `dim_customer`, `dim_product`, `fact_order_history` or `fact_order_item_history` |
| `batch_id` | The micro-batch id of the stream's checkpoint |
| `seconds`, `jobs` | The batch's wall time and the Spark jobs it ran, counted in the stream's own job group, so the other streams' jobs running at the same time are not counted |
| `written` | The rows computed for each table; a table with 0 gets no merge and no copy |
| `steps` | Each step in order, with its seconds and Spark jobs: `catch-up` (copying a table a failed batch left behind), `compute <table>` and `merge <table>` for each table, `snapshots` (reading the new snapshot ids, no Spark job) and `copy` (the JDBC staging and the PostgreSQL transaction) |

```sh
docker compose logs gold-job | grep 'GOLD BATCH'                                                   # every batch
docker compose logs --no-log-prefix gold-job | sed -n 's/.*GOLD BATCH //p' | jq -c '{load, seconds, jobs}'   # the totals, one line per batch
```

A replayed batch, after an Iceberg commit conflict, logs once: its steps list every attempt's steps, and its totals cover them all. The other log lines of the gold job are warnings and errors only.

## Alerts

`monitoring/prometheus/rules/spark.yml` defines two alert rules, each firing after 5 minutes. Prometheus cannot read the jobs' settings, so the same file records each pipeline job's trigger interval as `spark_streaming_trigger_seconds{service="<job>-job"}` (10 s for landing, 2 s for bronze and silver, 5 s for gold, as in `.env`); a unit test fails when the two disagree, so change both together.

| Alert | Fires when |
| --- | --- |
| `StreamingQueryFallingBehind` | A query's processing rate stays below its input rate: its backlog grows |
| `StreamingBatchOverTrigger` | A query's micro-batches take longer than its job's trigger interval (`spark_streaming_trigger_seconds`): batches start late |

They show in Prometheus at <http://localhost:9090/alerts>. No Alertmanager runs, so nobody is notified; production would route them to one. `tests/prometheus/spark_rules_test.yml` holds their `promtool` tests.

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

Spark has no metric for network latency or bandwidth. The stage exporter gives its two closest signals: the time tasks wait for shuffle blocks (`spark_stage_shuffle_fetch_wait_seconds_total`, charted as a share of run time) and the shuffle bytes read from other executors (`spark_stage_shuffle_read_remote_bytes_total`, against the local reads). Locally they understate the network:

- both workers are containers on one Docker bridge on one machine, so a "remote" read is a copy through the host's memory, without the latency, bandwidth limits or losses of a real network;
- with two workers each running half of the executors, about half of the shuffle bytes are read remotely whatever the job does, so the remote share says more about the number of workers than about the job.

Read them as relative signals, to compare jobs, partitionings and trigger intervals on the same machine, not as a forecast of a real network. On a real cluster, the remote share grows towards (n − 1)/n with n executors, and the fetch wait share is the one to watch.
