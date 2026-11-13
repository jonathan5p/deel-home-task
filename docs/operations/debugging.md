# Debugging

## Where to look

| To see | Look at |
| --- | --- |
| Whether every service is up and healthy | `docker compose ps` |
| A pipeline job's logs | `make pipeline-logs PIPELINE=<job>` (`landing`, `bronze`, `silver`, `gold` or `all`) |
| Any other service's logs | `docker compose logs -f <service>`, for example `api`, `grafana`, `kafka-connect`, `maintenance-job`, `gold-retention-job`, `data-quality-job`, `iceberg-rest` |
| A running Spark application | its application UI: landing <http://localhost:4040>, bronze 4041, silver 4042, gold 4045 ([running the pipeline](running-the-pipeline.md)); the master at <http://localhost:8080> |
| Past or crashed Spark runs | the [Spark History Server](../architecture/infrastructure.md#spark-history-server-optional) at <http://localhost:18080> (`make spark-history`) |
| Lake files, checkpoints and data quality reports | the SeaweedFS filer at <http://localhost:8888/buckets/lake/> |
| Lake tables and views | `make spark-sql` ([landing examples](../lakehouse/landing.md#inspecting-the-landing-zone-with-spark-sql), [silver queries](../lakehouse/silver.md)) |
| Gold in PostgreSQL | `docker compose exec analytics-db psql -U analytics -d analytics` |
| Kafka topics and messages | [Conduktor](#optional-kafka-monitoring), or the [message format](../lakehouse/source-and-cdc.md#message-format) |
| Whether the data is complete and consistent | `make data-quality` ([data quality checks](../lakehouse/data-quality.md)) |
| How long a source change takes to reach each layer | `make latency-probe` ([end-to-end latency](latency.md)) |

## Common errors

| Error | Where | Cause and fix |
| --- | --- | --- |
| `PATH_NOT_FOUND` | bronze job, landing Spark SQL views | The landing job has not written that topic's first file yet. Run landing first; Compose restarts the bronze job until the folder exists ([bronze](../lakehouse/bronze.md)). |
| `TABLE_OR_VIEW_NOT_FOUND`, `MissingSilverTablesError` | silver job, `lake-views` | The upstream tables do not exist yet. Run bronze (and landing) first, or `PIPELINE=all`; Compose restarts them ([silver](../lakehouse/silver.md)). |
| `port is already allocated` | `make spark-submit`, `make spark-sql` | The pipeline jobs hold ports 4040 and up. Pass another port, for example `SPARK_UI_PORT=4050` ([submitting a job](../architecture/infrastructure.md#submitting-a-job)). |
| `Some data may have been lost because they are not available in Kafka any more` | landing job | The landing checkpoint points past Kafka's offsets, for example after Kafka's topics were recreated. Reset the lake ([destructive reset](#destructive-reset)). |
| `CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE` | silver job | Its checkpoint has a different state layout. Reset silver ([resetting silver](../lakehouse/silver.md#resetting-silver)). |
| `Cannot load current offset at snapshot …` | silver or gold stream, `--available-now` runs | Lake maintenance expired the snapshot the stream resumes from. Reset that stream's checkpoint ([lake maintenance](maintenance-and-recovery.md#lake-maintenance)). |
| A changed init script has no effect | `analytics-db` | Init scripts run only on an empty volume ([schemas and init script](../architecture/infrastructure.md#schemas-and-init-script)). |
| A service login is refused | API, Grafana | Its credentials in `.env` changed. Run `make up`, then `make db-logins` ([service logins](../architecture/infrastructure.md#read-only-service-logins)). |

## Stopping and resetting

| Command | Effect |
| --- | --- |
| `make pipeline-stop PIPELINE=<job>` | Stops one job; the rest of the stack keeps running |
| `make down` | Stops every container and keeps all data |
| `make gold-reset CONFIRM=1` | Empties gold and its checkpoints; the rest of the lake stays ([gold](../lakehouse/gold/writer.md#the-gold-ddl)) |
| `make gold-rebuild CONFIRM=1` | Reloads gold from silver ([rebuilding gold](maintenance-and-recovery.md#rebuilding-gold-from-silver)) |
| `make clean CONFIRM=1` | Deletes every volume: all data is lost (below) |

## Destructive reset

**Data loss:** `make clean CONFIRM=1` removes this Compose project's containers and volumes: the source database, Kafka, the analytics database, the lake and the monitoring data. It deletes no images or source files and runs no global Docker prune. It refuses to run unless `CONFIRM` is exactly `1`.

`make down`, by contrast, keeps every volume, so all data survives a restart.

## Optional Kafka monitoring

With the stack running, start only the Conduktor services:

```sh
docker compose --profile monitoring up -d --no-deps --wait conduktor-db
docker compose --profile monitoring up -d --no-deps --wait conduktor-console
```

Open <http://localhost:8090> and sign in with `admin@local.test` / `LocalConsole123!` (public, development-only credentials; the port is published on all host interfaces). Console connects to the broker at `kafka:29092` and covers topic management and message browsing. Stop it without touching the rest:

```sh
docker compose --profile monitoring stop conduktor-console conduktor-db
```
