# Monitoring Spark

The optional `metrics` profile scrapes every Spark driver, the master and the workers into [Prometheus](https://prometheus.io/), and [Grafana](../serving/grafana.md) charts them. It answers two questions: are the streaming queries keeping up right now, and how much of the cluster do the jobs really use, to size it (see [the sizing strategy](../architecture/sizing.md#key-metrics)).

## Start it

```sh
make up PROFILES=metrics       # Prometheus on http://localhost:9090 and the Spark History Server
make pipeline PIPELINE=all     # Then the streaming jobs, if they are not running yet
```

Then open Grafana at <http://localhost:3000>: the **Spark and lake** folder holds the **Spark cluster** and **Spark streaming** dashboards, and the links bar at the top of each one moves to the other. The **Gold metrics** dashboard stays in the **Gold** folder.

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

Drivers are found by their Compose service name. Docker's DNS resolves `landing-job`, `bronze-job`, `silver-job`, `gold-job`, `lake-views`, `maintenance-job`, `data-quality-job` and `spark-app` to the containers running them, and Prometheus asks again every 10 seconds, so a driver is scraped from its first 10 seconds until it ends; the `service` label names it. The drivers started by `make spark-submit`, `make spark-sql` and the targets built on them join the network with the `spark-app` alias (`docker compose run --use-aliases`), so they are scraped too, under `service="spark-app"`.

The configuration in `monitoring/prometheus/prometheus.yml` also reshapes the metric names, so that they stay the same from one run to the next:

- `spark.metrics.namespace=spark` replaces the application id in the driver metric names (`metrics_spark_driver_...`), which would otherwise start a new series at every restart.
- A streaming query's metrics carry the query name in the metric name; they become `spark_streaming_<measure>{query="<query>"}` (`inputRate_total`, `processingRate_total`, `latency`, `states_rowsTotal`, `states_usedBytes`, `eventTime_watermark`), with the query name's spaces and dots turned into `_`.
- The applications page carries the application name and start time in the metric name; it becomes `spark_application_cores{app="<name>"}`.
- Executor metrics name the application in an `app` label, without the application id.
- Spark repeats every gauge as `_Number` and `_Value`; only `_Value` is kept, and the code generation histograms and listener timers are dropped.

## Alerts

`monitoring/prometheus/rules/spark.yml` defines two alert rules, each firing after 5 minutes:

| Alert | Fires when |
| --- | --- |
| `StreamingQueryFallingBehind` | A query's processing rate stays below its input rate: its backlog grows |
| `StreamingBatchOverTrigger` | A query's micro-batches take longer than the 10 s trigger: batches start late |

They show in Prometheus at <http://localhost:9090/alerts>. No Alertmanager runs, so nobody is notified; production would route them to one. `tests/prometheus/spark_rules_test.yml` holds their `promtool` tests.

## Read the panels for sizing

| Question | Dashboard → panel | Look for |
| --- | --- | --- |
| Is the cluster full? | Spark cluster → Worker cores used and free, Applications running and waiting | Free cores near zero while applications wait |
| Do the jobs use the cores they hold? | Spark cluster → Busy task slots per application | Busy slots well under the granted cores mean `spark.cores.max` can shrink |
| Is the executor memory enough? | Spark cluster → Executor peak heap and RSS vs 1 GiB, GC share of task time | Heap peaks near 1 GiB or GC above 10 % mean more memory or smaller partitions |
| Does each query keep up? | Spark streaming → Input vs processing rate, Processing / input ratio | Ratio at or above 1 |
| Is the trigger right? | Spark streaming → Batch duration vs the 10 s trigger, Share of time over the trigger | Batches well under the line; a growing share means a longer trigger or more cores |
| Does silver's state level off? | Spark streaming → State rows, State size, State growth | Growth falling towards zero once the deduplication window is full |

Use a time range that covers a burst of source changes and the quiet after it: the rates and durations are those of the last micro-batch, sampled every 10 seconds.

## Network-proxy limits

Spark has no metric for network latency or bandwidth. Its closest signals are the shuffle fetch wait time and the bytes read from remote executors, which the per-executor shuffle totals only hint at. Locally they understate the network: both workers share one Docker bridge on one machine, so a "remote" read never leaves the host. Read them as relative signals, to compare jobs and partitionings, not as a forecast of a real network.
