# Debugging

## Where to look

| To see | Look at |
| --- | --- |
| Whether every service is up and healthy | `docker compose ps` |
| A pipeline job's logs | `make pipeline-logs PIPELINE=<job>` (`landing`, `bronze`, `silver`, `gold` or `all`) |
| Any other service's logs | `docker compose logs -f <service>`, for example `api`, `grafana`, `kafka-connect`, `maintenance-job`, `gold-retention-job`, `data-quality-job`, `iceberg-rest` |
| A running Spark application | its application UI: landing <http://localhost:4040>, bronze 4041, silver 4042, gold 4045 ([running the pipeline](running-the-pipeline.md)); the master at <http://localhost:8080> |
| Past or crashed Spark runs | the [Spark History Server](../platform/spark.md#spark-history-server-optional) at <http://localhost:18080> (`make spark-history`) |
| Lake files, checkpoints and data quality reports | the SeaweedFS filer at <http://localhost:8888/buckets/lake/> |
| Lake tables and views | `make spark-sql` ([landing examples](../layers/landing.md#inspecting-the-landing-zone-with-spark-sql), [silver queries](../layers/silver.md)) |
| Gold in PostgreSQL | `docker compose exec analytics-db psql -U analytics -d analytics` |
| Kafka topics and messages | [Conduktor](#optional-kafka-monitoring), or the [message format](../layers/source-and-cdc.md#message-format) |
| Whether the data is complete and consistent | `make data-quality` ([data quality checks](data-quality.md)) |

## Common errors

| Error | Where | Cause and fix |
| --- | --- | --- |
| `PATH_NOT_FOUND` | bronze job, landing Spark SQL views | The landing job has not written that topic's first file yet. Run landing first; Compose restarts the bronze job until the folder exists ([bronze](../layers/bronze.md)). |
| `TABLE_OR_VIEW_NOT_FOUND`, `MissingSilverTablesError` | silver job, `lake-views` | The upstream tables do not exist yet. Run bronze (and landing) first, or `PIPELINE=all`; Compose restarts them ([silver](../layers/silver.md)). |
| `port is already allocated` | `make spark-submit`, `make spark-sql` | The pipeline jobs hold ports 4040 and up. Pass another port, for example `SPARK_UI_PORT=4050` ([submitting a job](../platform/spark.md#submitting-a-job)). |
| `Some data may have been lost because they are not available in Kafka any more` | landing job | The landing checkpoint points past Kafka's offsets, after a stack started before Kafka had volumes. Reset the lake ([destructive reset](#destructive-reset)). |
| `CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE` | silver job | Its checkpoints were written before deduplication. Reset silver ([silver](../layers/silver.md)). |
| `Cannot load current offset at snapshot …` | silver or gold stream, `--available-now` runs | Lake maintenance expired the snapshot the stream resumes from. Reset that stream's checkpoint ([lake maintenance](maintenance-and-recovery.md#lake-maintenance)). |
| Appends to bronze fail on `change_position` | bronze job | The tables predate the `_` metadata columns. Drop them with their checkpoints ([bronze](../layers/bronze.md)). |
| A changed init script has no effect | `analytics-db` | Init scripts run only on an empty volume ([schemas and init script](../platform/services.md#schemas-and-init-script)). |
| A service login is refused | API, Grafana | Its credentials in `.env` changed. Run `make up`, then `make db-logins` ([service logins](../platform/services.md#read-only-service-logins)). |

## Stopping and resetting

| Command | Effect |
| --- | --- |
| `make pipeline-stop PIPELINE=<job>` | Stops one job; the rest of the stack keeps running |
| `make down` | Stops every container and keeps all data |
| `make gold-reset CONFIRM=1` | Empties gold and its checkpoints; the lake stays ([gold](../layers/gold.md#gold-in-postgresql)) |
| `make gold-rebuild CONFIRM=1` | Reloads gold from silver ([rebuilding gold](maintenance-and-recovery.md#rebuilding-gold-from-silver)) |
| `make clean CONFIRM=1` | Deletes every volume: all data is lost (below) |

## Destructive reset

**Data loss:** `make clean CONFIRM=1` removes this Compose project's containers and volumes, including persisted database, Kafka, and optional monitor data. Use it only when you intend to discard that data. It does not delete images or source files, or perform a global Docker prune. `make clean` refuses before invoking Docker unless GNU Make evaluates `CONFIRM` to exactly `1`. Make normalizes leading assignment whitespace, so `make clean 'CONFIRM= 1'` also explicitly confirms deletion.

All lifecycle targets use the same Compose project selection; `make down` preserves its volumes, so the source database, the Kafka topics (including the offsets Debezium stores in Kafka), the analytics database and the lake survive a restart. Keep the same working directory and Compose environment between commands. `COMPOSE` defaults to `docker compose` and can be overridden for a compatible CLI or a harmless test probe. Compose failures cause Make to fail.

**Stacks started before Kafka had volumes** kept their topics only inside the Kafka container, so the first `make up` with the volumes starts with empty topics and Debezium snapshots the source tables again. Landing then stops with `Some data may have been lost because they are not available in Kafka any more`, because its checkpoint points past the new offsets: reset the lake once with `make clean CONFIRM=1`, or drop its tables and checkpoints, so the pipelines rebuild from the new topics.

## Optional Kafka monitoring

With the source stack already running, start only the optional Conduktor services:

```sh
docker compose --profile monitoring up -d --no-deps --wait conduktor-db
docker compose --profile monitoring up -d --no-deps --wait conduktor-console
```

Open <http://localhost:8090> and sign in with `admin@local.test` / `LocalConsole123!`. These are public development-only credentials. The current `8090:8080` mapping publishes on all host interfaces; use `127.0.0.1:8090:8080` if localhost-only binding is required. Console connects to the existing broker at `kafka:29092`.

Only Console and its metadata PostgreSQL database are added. Topic management, message browsing/production, and basic cluster inspection do not require Cortex. Historical metrics dashboards and alerting are not included; no ksqlDB service is required.

Stop only monitoring, keeping the source stack and stored data:

```sh
docker compose --profile monitoring stop conduktor-console conduktor-db
```

The Conduktor integration is prepared but runtime validation is still pending. `make down` stops the entire project; use the service-specific command above when leaving the source running.
