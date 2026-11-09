## Data Engineering Take-Home Task

### Welcome

Welcome to Deel's Data Engineering Take-Home task, as mentioned in the Task specification document, this is the pre-built stack that will help you on your solution development. This repository contains a pre-configured database containing the database represented by the following DER:


![Database Diagram](./diagrams/database-diagram.png)


### Database Configuration

Install [Docker](https://www.docker.com/products/docker-desktop/) with [Compose v2](https://docs.docker.com/compose/install/) and GNU Make. With the Docker engine running, run these commands from the repository root:

```sh
make up             # Build changed local images and start the default stack (source/CDC, analytics database, data lake, Spark cluster)
make up PROFILES="monitoring history"  # Also start optional services (any combination of profiles)
make down           # Stop/remove all project containers, including optional profiles; keep volumes
make spark-submit   # Submit a job to the Spark cluster (defaults to a one-minute UI check job)
make help           # Show lifecycle commands
```

Optional services are grouped in Compose profiles and are not started by a plain `make up`: `monitoring` (Conduktor, see **Optional Kafka monitoring**) and `history` (Spark History Server, see **Spark History Server**). Pass them with `PROFILES`, separated by spaces.

#### Destructive reset

**Data loss:** `make clean CONFIRM=1` removes this Compose project's containers and volumes, including persisted database, Kafka, and optional monitor data. Use it only when you intend to discard that data. It does not delete images or source files, or perform a global Docker prune. `make clean` refuses before invoking Docker unless GNU Make evaluates `CONFIRM` to exactly `1`. Make normalizes leading assignment whitespace, so `make clean 'CONFIRM= 1'` also explicitly confirms deletion.

All lifecycle targets use the same Compose project selection; `make down` preserves its volumes. Keep the same working directory and Compose environment between commands. `COMPOSE` defaults to `docker compose` and can be overridden for a compatible CLI or a harmless test probe. Compose failures cause Make to fail.

Once you have the Database up and running feel free to connect to this using any tool you want, for this you can use the following credentials:

- **Username**: `finance_db_user`
- **Password**: `1234`
- **Database**: `finance_db`

### Analytics database

A separate PostgreSQL instance (`analytics-db`) holds the gold layer and the Iceberg catalog, so analytical load never reaches the source database. It starts with `make up` and keeps its data in the `analytics-pg-data` volume across `make down` / `make up`.

| Setting | From the host | From other containers |
| --- | --- | --- |
| Host and port | `localhost:5433` | `analytics-db:5432` |
| Username | `analytics` | `analytics` |
| Password | `analytics` | `analytics` |
| Database | `analytics` | `analytics` |

The credentials are local development defaults in `.env` (`ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD`, `ANALYTICS_DB_NAME`); variables exported in the shell override them.

```sh
docker compose exec analytics-db psql -U analytics -d analytics
```

#### Schemas and init script

`docker/analytics-db/initdb/01-gold-and-catalog.sql` creates:

- the `gold` schema in the `analytics` database, for the dimensional model;
- the `iceberg_catalog` database, where the Iceberg JDBC catalog creates its own tables on first use.

PostgreSQL runs init scripts **only when the data volume is empty**, so editing the script does not change an existing database. To apply a changed script, recreate only the analytics volume. **Data loss:** this deletes all gold data and the Iceberg catalog, which then no longer points to the lake tables.

```sh
docker compose down -v analytics-db  # Removes only the analytics container and its volume
make up
```

#### Gold layer

The `gold` schema holds a star schema built from the silver history tables: SCD2 customer and product dimensions, seeded date and order-status dimensions, append-only history facts for orders and order items (partitioned by month, 3 years kept), and current-state tables for orders and items that the four business questions read. The design, with the ERD, the grain of every table, the open and pending rules, versioning by `change_position`, and retention, is in [docs/architecture/gold-model.md](docs/architecture/gold-model.md).

In short:

- **Open order:** current status other than `COMPLETED`, not deleted. **Pending orders** are open orders, and **pending items** are the items of open orders.
- **Current state only moves forward:** a version replaces the current row only if its `change_position` is higher, so late or replayed events never overwrite newer state; they are still kept as history.
- **Top 3 questions include ties**, so more than three rows can come back.

### Data lake (SeaweedFS)

[SeaweedFS](https://github.com/seaweedfs/seaweedfs) provides the S3-compatible object store for the data lake. It runs as a single `seaweedfs` container (`weed mini`), starts with `make up`, and keeps its data in the `seaweedfs-data` volume. It creates the `lake` bucket on startup if it does not exist, and the service reports healthy only once the bucket is there.

| Endpoint | From the host | From other containers |
| --- | --- | --- |
| S3 API | `http://localhost:8333` | `http://seaweedfs:8333` |
| Filer UI (browse buckets and files) | http://localhost:8888/buckets/lake/ | `http://seaweedfs:8888` |

| Setting | Value |
| --- | --- |
| Access key | `lakeadmin` |
| Secret key | `lakeadmin-secret` |
| Region | any (for example `us-east-1`); use path-style access |

The credentials are local development defaults in `.env` (`LAKE_S3_ACCESS_KEY`, `LAKE_S3_SECRET_KEY`); variables exported in the shell override them. Any S3-compatible store, or AWS S3 itself, can replace SeaweedFS by changing the endpoint and credentials.

#### Iceberg catalog

Every Spark job gets an [Apache Iceberg](https://iceberg.apache.org/) catalog named `lake`, configured in `docker/spark/spark-defaults.conf`:

- table metadata (the catalog) lives in the `iceberg_catalog` database of `analytics-db`, through Iceberg's JDBC catalog;
- table data and metadata files live in SeaweedFS under `s3://lake/warehouse/<namespace>/<table>/`.

The Spark image bundles Iceberg 1.12.0 (`iceberg-spark-runtime-4.1_2.13`, `iceberg-aws-bundle`) and the PostgreSQL JDBC driver 42.7.14, with pinned checksums, so jobs need no `--packages`. The driver reads the database and S3 credentials from its environment (the `.env` values above).

#### Kafka, Avro and plain files in the lake

The image also bundles, with pinned checksums:

| Library | Version | Used for |
| --- | --- | --- |
| `spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`, `kafka-clients`, `commons-pool2` | 4.1.4, 4.1.4, 3.9.1, 2.12.1 | Reading the CDC topics with `spark.readStream.format("kafka")` |
| `spark-avro` | 4.1.4 | Reading and writing Avro files |
| `hadoop-aws` and the AWS SDK's `s3-transfer-manager` and `apache-client` | 3.4.2, 2.54.17 | The S3A filesystem for plain files and streaming checkpoints in SeaweedFS |

Iceberg tables use `s3://` paths through the catalog above. Plain files, such as the landing zone, and streaming checkpoints use `s3a://lake/...` paths. S3A uses the same AWS SDK version as the Iceberg bundle (2.54.17); the image adds only the two SDK modules the bundle does not contain.

Spark does not substitute `${env:...}` in `spark.hadoop.*` settings, so `make spark-submit` passes the S3A credentials from the driver container's environment (`LAKE_S3_ACCESS_KEY`, `LAKE_S3_SECRET_KEY`). Spark ships them to the executors and shows them as `*********(redacted)` in the UI and event logs.

Run the smoke job to check the whole path. It creates or replaces `lake.smoke.iceberg_check`, writes four rows, reads them back, and fails if they differ:

```sh
make spark-submit SPARK_APP=tests/infra/apps/iceberg_smoke_check.py
```

To inspect the result:

```sh
docker compose exec analytics-db psql -U analytics -d iceberg_catalog -c "select * from iceberg_tables"
```

The table files are visible at http://localhost:8888/buckets/lake/warehouse/smoke/iceberg_check/.

### Optional Kafka monitoring

With the source stack already running, start only the optional Conduktor services:

```sh
docker compose --profile monitoring up -d --no-deps --wait conduktor-db
docker compose --profile monitoring up -d --no-deps --wait conduktor-console
```

Open http://localhost:8090 and sign in with `admin@local.test` / `LocalConsole123!`. These are public development-only credentials. The current `8090:8080` mapping publishes on all host interfaces; use `127.0.0.1:8090:8080` if localhost-only binding is required. Console connects to the existing broker at `kafka:29092`.

Only Console and its metadata PostgreSQL database are added. Topic management, message browsing/production, and basic cluster inspection do not require Cortex. Historical metrics dashboards and alerting are not included; no ksqlDB service is required.

Stop only monitoring, keeping the source stack and stored data:

```sh
docker compose --profile monitoring stop conduktor-console conduktor-db
```

The Conduktor integration is prepared but runtime validation is still pending. `make down` stops the entire project; use the service-specific command above when leaving the source running.

### Debezium CDC

The stack includes a Debezium CDC pipeline that streams database changes to Kafka in real-time. Kafka is available at `localhost:9092`.

#### Topics

| Kafka Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

When the connector starts, it first takes a snapshot of each table (`op` = `r`) and then streams every change. The source database and the connector are used exactly as provided; the pipeline handles their format.

#### Message format

Each topic has one partition, messages carry no headers, and both key and value are JSON without embedded schemas.

- **Key:** the row's primary key, for example `{"order_id": 1}`.
- **Value:** a Debezium change event:

| Field | Meaning |
|---|---|
| `before` | The row before the change. `null` for snapshot reads and inserts; the full row for updates and deletes, because the source tables use `REPLICA IDENTITY FULL`. |
| `after` | The row after the change. `null` for deletes. |
| `op` | `r` snapshot read, `c` insert, `u` update, `d` delete. |
| `ts_ms` | When Debezium processed the change, in epoch milliseconds. |
| `source` | Origin of the change: `table`, `txId`, `lsn` (position in the source's write-ahead log, increasing in commit order), `ts_ms` (commit time in the source) and `snapshot` (`first`, `true`, `last` and similar while snapshotting; `false` for streamed changes). |
| `transaction` | Always `null`: transaction metadata is not enabled. |

An update to an order, abbreviated:

```json
{
  "before": {"order_id": 1, "order_date": 20765, "delivery_date": 20775, "status": "PENDING", "updated_at": 1794147633457, "...": "..."},
  "after":  {"order_id": 1, "order_date": 20765, "delivery_date": 20775, "status": "REPROCESSING", "updated_at": 1794147660022, "...": "..."},
  "source": {"connector": "postgresql", "table": "orders", "txId": 952, "lsn": 26767664, "ts_ms": 1794147660025, "snapshot": "false", "...": "..."},
  "op": "u",
  "ts_ms": 1794147660561,
  "transaction": null
}
```

Column values are encoded by type:

| Source type | Columns | Encoding | Example | Reading it in Spark |
|---|---|---|---|---|
| `BIGSERIAL`, `BIGINT`, `INTEGER` | ids, `updated_by`, `created_by`, `quanity` | JSON number | `157` | cast to `long` or `int` |
| `VARCHAR` | names, `barcode`, `status`, `customer_address` | JSON string | `"PENDING"` | as is |
| `BOOLEAN` | `is_active` | JSON boolean | `true` | as is |
| `TIMESTAMP(3)` | `updated_at`, `created_at` | Milliseconds since the epoch of the stored wall-clock time, read as UTC (the source runs in UTC) | `1794147660022` = 2026-11-08 14:21:00.022 | `timestamp_millis(col)`, with the session time zone set to UTC |
| `DATE` | `order_date`, `delivery_date` | Days since 1970-01-01 | `20775` = 2026-11-18 | `date_from_unix_date(col)` |
| `DECIMAL` (no precision or scale) | `unity_price` | Debezium's variable-scale decimal: an object with the `scale` and the unscaled value as base64 big-endian two's-complement bytes | `{"scale": 2, "value": "AJk="}` = 153 × 10⁻² = 1.53 | decode the bytes to an integer and divide by 10^`scale` |

Column names are kept as in the source, including `order_items.quanity`.

**Deletes:** the data generator never deletes rows. If a row is deleted, Debezium sends an event with `op` = `d`, the full row in `before` and `after` = `null`, followed by a *tombstone*: a message with the same key and a `null` value. Consumers must accept `null` values.

**Heartbeats:** every 10 seconds the connector writes `{"ts_ms": ...}` with the key `{"serverName": "finance_db"}` to `__debezium-heartbeat.finance_db`, so the replication slot keeps advancing while the tables are idle. The pipeline does not read this topic.

#### Kafka Connection Example

```properties
bootstrap.servers=localhost:9092
```

### Spark cluster

`make up` also starts a Spark 4.1.4 standalone cluster: one master and two workers built from the `spark` target of `docker/spark/Dockerfile`. Drivers run from the `app` target, which is built on top of the `spark` target and adds the application packages, so drivers and executors always share Spark, Java and Python versions.

| Service | UI | Resources |
|---|---|---|
| `spark-master` | http://localhost:8080 | Cluster manager; accepts jobs at `SPARK_MASTER_URL` |
| `spark-worker-1` | http://localhost:8081 | 2 cores, 2 GiB executor memory |
| `spark-worker-2` | http://localhost:8082 | 2 cores, 2 GiB executor memory |
| `spark-history` (optional) | http://localhost:18080 | History server for finished, crashed and running applications; started with `make spark-history` |
| `spark-app` (one container per run) | http://localhost:4040 | Driver of an application started by `make spark-submit`; its UI is only available while it runs |
| `landing-job` (optional) | http://localhost:4040 | Driver of the landing job, a long-running service started by `make pipeline` (see [Running the pipeline](#running-the-pipeline)) |

Spark runs PySpark on **Python 3.14** (3.14.10), the newest version PySpark 4.1.4 supports. The official `apache/spark` images ship Python 3.10, so the Dockerfile installs CPython 3.14 with [uv](https://docs.astral.sh/uv/) at build time and points `PYSPARK_PYTHON` at it; every driver and executor uses it.

Worker resources are set with `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` in `docker-compose.yaml`. They cap what executors can claim on each worker; the master, worker and driver JVMs need memory on top of that, so make sure Docker has enough memory for the whole stack.

`SPARK_MASTER_URL` defaults to `spark://spark-master:7077` in `.env`, which Docker Compose loads automatically. Exporting the variable in your shell overrides it; recreate the Spark services (`make up`) after changing it.

`.env` also pins the Compose project name (`COMPOSE_PROJECT_NAME=deel_home_task`), so every checkout of the repository, including git worktrees, drives the same local stack. `make up` rebuilds the Spark images from the checkout it runs in. A rebuild without changes keeps the image IDs, so running `make up` again does not restart the cluster or the jobs running on it. Two things keep the IDs stable: each Spark image has a single builder (`spark-master` builds the cluster image, which the workers and the History Server reuse; `spark-app` builds the driver image, which the pipeline jobs reuse), and the Makefile turns off build attestations (`BUILDX_NO_DEFAULT_ATTESTATIONS=1`), which would otherwise give every build a new ID.

#### Submitting a job

```sh
make spark-submit                                                               # Run the one-minute UI check job (default)
make spark-submit SPARK_APP=tests/infra/apps/iceberg_smoke_check.py             # Run another application
make spark-submit SPARK_APP=path/to/app.py SPARK_APP_ARGS="--date 2026-11-05"  # Pass arguments to the application
make spark-submit SPARK_CONF="spark.cores.max=2" SPARK_UI_PORT=4041             # Cap cores and publish the UI on another port
```

| Variable | Default | Meaning |
|---|---|---|
| `SPARK_APP` | `tests/infra/apps/spark_ui_check.py` | Application to run, as a path from the repository root |
| `SPARK_APP_ARGS` | empty | Arguments passed to the application unchanged |
| `SPARK_CONF` | empty | Space-separated `key=value` Spark settings, each passed as `--conf` |
| `SPARK_UI_PORT` | `4040` | Host port for the application UI; `0` picks a free port |

Each run starts its own driver container from the `app` image with `docker compose run --rm`, in client mode, and the container is removed when the application ends. A batch job's container ends with the job; a streaming job's container lives as long as the stream. `make spark-submit` rebuilds the `app` image first when the code changed, and the cluster must already be running (`make up`).

```mermaid
flowchart LR
    make["make spark-submit"] -->|docker compose run --rm| driver["spark-app container<br/>driver, UI on :4040"]
    driver -->|1. registers the application| master["spark-master<br/>:7077"]
    master -->|2. starts executors| workers["spark-worker-1 / spark-worker-2"]
    workers -->|3. executors connect back,<br/>run tasks, return results| driver
```

Because every run has its own container, one application's memory, logs and lifecycle never affect another, and stopping `make spark-submit` (Ctrl+C) stops its driver. Executors reach the driver through the container's IP address, and the `common` and `pipelines` packages are shipped to them with `--py-files`.

To run several applications at the same time, give each its own UI port and cap its cores: the standalone master gives an application every free core unless `spark.cores.max` is set, so a second application would wait until the first one ends.

While the landing job runs (`make pipeline`), it holds port 4040, so give ad-hoc runs another port, for example `make spark-submit SPARK_UI_PORT=4042` (the same applies to `make spark-sql`); otherwise Docker refuses to start the driver with `port is already allocated`.

#### Spark History Server (optional)

Every driver writes event logs to the `spark-events` volume, whether or not the history server is running. Logs roll over at 128 MiB, so long-running streaming applications stay in bounded files. Start the history server only when you need to inspect past runs:

```sh
make spark-history          # Start the history server if it is stopped (http://localhost:18080), stop it if it is running
make up PROFILES=history    # Or start it together with the rest of the stack
```

`make spark-history` is a toggle: run it once to start the history server and again to stop and remove its container. The event logs stay in the volume either way.

When it starts, it lists every application whose event logs are in the volume, including runs that happened while it was stopped. `make down` stops it with the rest of the stack, and `make clean CONFIRM=1` deletes the event logs.

#### Checking the application UI

The application UI at http://localhost:4040 exists only while a driver runs. The default job, [`tests/infra/apps/spark_ui_check.py`](tests/infra/apps/spark_ui_check.py), is a dummy streaming query that runs for one minute so the UI can be inspected. It reads Spark's built-in `rate` source and counts rows per 10-second window every five seconds. While the job runs, open http://localhost:4040:

| Tab | What to look at |
|---|---|
| Jobs and Stages | One job per micro-batch; the windowed count adds a shuffle stage |
| SQL / DataFrame | The plan of each micro-batch |
| Executors | Executors on both workers |
| Structured Streaming | The `rate_window_counts` query, with input and processing rates and batch durations |

The job prints the progress of the latest micro-batch every five seconds. When it finishes, the UI on port 4040 closes.

#### Inspecting the landing zone with Spark SQL

`make spark-sql` opens the Spark SQL shell in its own driver container, like `make spark-submit`: it runs on the cluster, reads the lake with the S3A credentials, and can use the Iceberg catalog `lake` (Spark loads it on first use, so it appears in `SHOW CATALOGS` only after a query such as `SHOW NAMESPACES IN lake;`). It uses Spark's in-memory session catalog, so temporary views live for the session only.

```sh
make spark-sql                                                      # interactive shell
make spark-sql SPARK_SQL_FILE=tools/spark-sql/landing_examples.sql \
  SPARK_SQL_VARS="topic=finance_db.operations.orders key_field=order_id key_value=1 from_offset=0 to_offset=99"
```

[`tools/spark-sql/landing_examples.sql`](tools/spark-sql/landing_examples.sql) inspects one topic at a time and contains no topic or field names. Its variables:

| Variable | Set with | Meaning |
|---|---|---|
| `landing_root` | `LANDING_ROOT` (default `s3a://lake/landing`) | Root of the landing zone |
| `topic` | `SPARK_SQL_VARS` | Topic folder to inspect |
| `key_field`, `key_value` | `SPARK_SQL_VARS` | Key field and value for the `by key` example |
| `from_offset`, `to_offset` | `SPARK_SQL_VARS` | Offset range for the `offset range` example (both ends included) |

It defines a `landing` view that decodes the topic's records: the source coordinates (`record_source`, `source_partition`, `source_offset`, `published_at`), the key as JSON, `change_op` (`create`, `update`, `delete`, `snapshot`), and `before_json`/`after_json`. Tombstones have `tombstone = true` and no operation. Its examples return:

| Example | Returns |
|---|---|
| `latest` | the latest records |
| `by key` | every change of one key, in log order |
| `offset range` | an offset range |
| `last hour` | records published in the last hour |
| `counts` | record counts by operation |
| `tombstones` | tombstone records |

When a whole file runs, the result sets follow each other without headers, so every example's first column is its name. In the shell, set the variables first (for example `SET hivevar:topic=finance_db.operations.orders;`), then paste the views and the queries you need.

The default landing root only has data once the landing job has run; until then the views fail with `PATH_NOT_FOUND`.

Spark reads each topic folder through its file-sink commit log (`_spark_metadata`), so files from failed micro-batches are ignored and counts are exact.

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.

For any questions, feel free to reach us out through data-platform@deel.com

### Application code

The Python code is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one repository-wide lock file and toolchain, with one small project per component under `analytics/`. Each image installs only the project it runs and what that project depends on.

```text
pyproject.toml              workspace root: members, development tools and their settings
uv.lock                     locked versions for the whole workspace (install with uv sync --frozen)
.python-version             Python 3.14.10, the same version the Spark image runs
analytics/
  common/                   project "common": code shared by the components
    src/common/settings.py  shared settings (analytics database) and environment helpers
    tests/
  pipelines/                project "pipelines": the Spark pipelines, depends on common
    src/pipelines/
      settings.py           pipeline settings (Kafka, lake, analytics database)
      schemas.py            the landing record schema
      quality/checks.py     the data quality check framework
      landing/              landing layer: job.py (streaming job), quality.py and quality_job.py (data quality checks)
      bronze/               bronze layer: source entities, the transforms from landing records to bronze rows, and job.py (streaming job)
    tests/
tests/infra/                infra tests that drive an isolated copy of the stack
  apps/                     Spark applications they submit (UI check, Iceberg smoke check, version and hold apps, fixture helpers)
```

Pipeline code is grouped by layer, `pipelines/<layer>/` (for example `pipelines/landing/job.py` for the landing job). Tests mirror the source tree of their project: `analytics/<project>/src/<project>/<path>/<module>.py` is tested in `analytics/<project>/tests/<path>/test_<module>.py`.

PySpark is not a dependency: it comes from the Spark image, so the pipelines always run the cluster's exact Spark version.

To work on the code outside Docker, install uv and run:

```sh
uv sync --frozen          # create .venv with Python 3.14.10, every project and the dev tools
uv run pytest --no-cov    # run the tests that do not need Spark
```

Outside Docker the Spark tests are skipped, so coverage is only meaningful with `make test`, which runs every test in the image.

#### Configuration

Settings are read from environment variables into small, immutable settings objects, one per concern. The environment is always passed in explicitly, so code and tests never read or patch the process environment:

```python
import os

from pipelines.settings import PipelineSettings

settings = PipelineSettings.from_env(os.environ)  # only a job's entry point reads os.environ
```

The code holds no deployment details: anything that depends on where it runs (brokers, topics, the lake, the catalog, the database) is a required variable with no default. The local values live in `.env`, and Docker Compose passes them to the `spark-app` driver containers and the pipeline job services; another environment sets its own values. Only tuning settings have defaults; set them in the shell for one run, for example `LANDING_TRIGGER_SECONDS=30 make spark-submit ...`.

| Variable | Settings | Default | Local value in `.env` | Purpose |
| --- | --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `pipelines.settings.KafkaSettings` | required | `kafka:29092` | Kafka brokers to read CDC topics from |
| `KAFKA_TOPICS` | `pipelines.settings.KafkaSettings` | required | the four `finance_db.operations.*` topics | Comma-separated CDC topics to read |
| `LAKE_ROOT` | `pipelines.settings.LakeSettings` | required | `s3a://lake` | Root of the data lake for plain files (S3A) |
| `ICEBERG_CATALOG` | `pipelines.settings.LakeSettings` | required | `lake` | Spark catalog name for Iceberg tables |
| `CHECKPOINT_ROOT` | `pipelines.settings.LakeSettings` | `<LAKE_ROOT>/checkpoints` | not set | Root for Structured Streaming checkpoints |
| `LANDING_TRIGGER_SECONDS` | `pipelines.settings.LandingSettings` | `10` | not set | Micro-batch interval of the landing job (a positive integer) |
| `BRONZE_TRIGGER_SECONDS` | `pipelines.settings.BronzeSettings` | `10` | not set | Micro-batch interval of the bronze job (a positive integer) |
| `LANDING_CHECK_LAST_OFFSETS` | `pipelines.settings.LandingQualitySettings` | all landed offsets | not set | Limit the landing data quality checks to the latest offsets of each partition (a positive integer) |
| `ANALYTICS_DB_URL` | `common.settings.AnalyticsDbSettings` | required | `jdbc:postgresql://analytics-db:5432/analytics` | JDBC URL of the analytics database (gold layer) |
| `ANALYTICS_DB_USER` | `common.settings.AnalyticsDbSettings` | required | `analytics` | Analytics database user |
| `ANALYTICS_DB_PASSWORD` | `common.settings.AnalyticsDbSettings` | required | `analytics` | Analytics database password |

`PipelineSettings` combines the Kafka, lake and analytics database settings. `LandingSettings` combines the Kafka and lake settings with the trigger interval and needs no database credentials. `BronzeSettings` needs only `KAFKA_TOPICS`, the lake settings and its trigger interval: it reads the landing zone, not Kafka. A missing or empty required variable raises `MissingSettingsError`, which lists every missing name of the settings being built at once; a malformed value, such as an empty topic list or a trigger interval that is not a positive integer, raises `InvalidSettingError`.

#### Landing zone

The landing job (`analytics/pipelines/src/pipelines/landing/job.py`) streams every record of the CDC topics into raw Avro files, exactly as Kafka delivered it, so the lake keeps a replayable copy of the sources:

```sh
make pipeline PIPELINE=landing   # as a long-running service; see Running the pipeline below
make spark-submit SPARK_APP=analytics/pipelines/src/pipelines/landing/job.py SPARK_CONF="spark.cores.max=2" SPARK_UI_PORT=4042   # or ad hoc, in the foreground
```

- **One streaming query per topic,** reading from the earliest offset and writing to `<LAKE_ROOT>/landing/<topic>/`: one folder per topic, with no partition folders.
- **Each record is stored unchanged:** `key` and `value` as raw bytes, plus `topic`, `partition`, `offset`, `timestamp` and `timestampType`. Tombstones keep their `null` value.
- **Restart-safe:** each query keeps its checkpoint in `<CHECKPOINT_ROOT>/landing/<topic>/`, and Spark's file sink records the committed files in `_spark_metadata`. Stopping and restarting the job neither skips nor duplicates records; Spark readers of the folder see only committed files.
- **Micro-batches** run every `LANDING_TRIGGER_SECONDS`. `spark.cores.max` keeps cores free for the other jobs; the job runs until it is stopped.

#### Running the pipeline

`make pipeline` runs the streaming jobs as long-running Compose services (profile `pipeline`), one driver container per job. Name the job with `PIPELINE`, or use `all` for every job:

```sh
make pipeline PIPELINE=landing        # start the stack, the History Server and the landing job
make pipeline PIPELINE=all            # the same with every job
make pipeline-logs PIPELINE=landing   # follow the job's logs (Ctrl+C stops following, not the job)
make pipeline-stop PIPELINE=landing   # stop and remove the job; the rest of the stack keeps running
```

`make pipeline` rebuilds changed images like `make up`, starts the default stack and the Spark History Server, then starts the selected jobs. Running it again after a code change restarts only the jobs whose image changed. A change to the Spark image restarts the cluster, and a job's driver does not survive that; `make pipeline` restarts the jobs along with it, but after a plain `make up`, run `make pipeline` again. Without a valid `PIPELINE` (`landing` or `all`), the targets fail and list the valid names. `make down` stops the jobs with the rest of the stack.

| Job | Service | Application UI |
| --- | --- | --- |
| landing | `landing-job` | http://localhost:4040 |

Where to look while a job runs:

- **Application UI** on the job's port (table above), including the Structured Streaming tab with one query per topic.
- **History Server** at http://localhost:18080, for current and past runs, since every driver writes event logs to the `spark-events` volume.
- **Landing files** in the SeaweedFS filer UI at http://localhost:8888/buckets/lake/landing/, one folder per topic.

Each job service submits its application with the same settings as `make spark-submit`, plus `spark.cores.max=2` and the service name as the driver host, so executors on the workers can connect back to it. The services restart unless they were stopped (`restart: unless-stopped`): a job whose driver fails is restarted and resumes from its checkpoints, without skipping or duplicating records. A container stopped with `make pipeline-stop` or `docker stop`/`docker kill` stays stopped until the next `make pipeline`.

The cluster has two workers with 2 cores each, and each job takes at most 2 cores. With the landing job and a second job running, every core is in use, so ad-hoc runs (`make spark-submit`, `make spark-sql`) wait for free cores until a job is stopped.

#### Data quality checks

Data quality checks verify the data itself, on the running stack. They are separate from unit tests (the code, on synthetic data) and from infra tests (the platform, on a throwaway stack), and run on demand. The framework lives in `analytics/pipelines/src/pipelines/quality/`, and each layer's checks next to its code (`pipelines/landing/quality.py`); they run as a Spark job:

```sh
make data-quality
```

The landing checks reconcile the landing zone with Kafka for every configured topic and every topic partition. Landed records are read through Spark, so only files committed by the file sink count; files in a topic folder that `_spark_metadata` does not record are ignored. Kafka is the reference, and it is read once per run into one snapshot, so every check sees the same records:

| Check | Checks that |
| --- | --- |
| `topic_found` | a configured topic has records in Kafka or in the landing zone |
| `partition_landed` | every Kafka partition has landed records |
| `unique_offsets` | every offset landed exactly once |
| `contiguous_offsets` | every offset Kafka holds in the compared range landed |
| `starts_at_kafka_min_offset` | landing starts at or before Kafka's earliest retained offset |
| `same_bytes_as_kafka` | landed keys and values equal Kafka's, and Kafka holds every landed offset it still retains |

- **Contiguity is measured against the offsets Kafka actually holds,** not against a dense range of numbers: transaction markers and compaction leave offsets with no record, and those are not gaps. Kafka offsets after the last landed offset are lag, not gaps.
- **Retention:** landed offsets that Kafka no longer retains are not counted as missing in Kafka, including in a partition whose records have all expired.
- **Empty runs fail:** a configured topic with no records anywhere fails `topic_found`, and a run with no results fails.
- **Schema drift fails loudly:** a topic folder whose committed files do not hold the Kafka record columns and types raises an error instead of reading as empty. Only a missing folder, or one without committed files, reads as no records.

Each result is printed as one JSON line prefixed with `LANDING QUALITY`, with the check's description, followed by a summary line, and the job exits with code 1 if any check fails. The `partition_landed` result also reports the row count, the offset range, the number of tombstones, Kafka's offset range (`kafka_min_offset`, `kafka_max_offset`) and the lag behind Kafka; lag is expected while landing runs and is not a failure. The `contiguous_offsets` result reports the compared range (`from_offset`, `to_offset`) and the number of `missing_offsets`.

By default every landed offset is compared, from the first to the last landed offset of each partition. For large topics, `LANDING_CHECK_LAST_OFFSETS=<n> make data-quality` limits the comparison to a window of the last `n` offsets up to the last landed offset of each partition, and skips `starts_at_kafka_min_offset`. The window limits what is compared; Kafka is still read in full. The job runs with `spark.cores.max=2` and a random UI port, so it can run while the landing job is running.

#### Bronze layer

Bronze is a typed, faithful log of every change event: one row per event, no deduplication. The package `analytics/pipelines/src/pipelines/bronze/` holds:

- `entities.py`: one `Entity` per source table, with its key, content and audit columns, a description of each column, its NOT NULL columns and the JSON row schema Debezium emits. `entity_for_topic` resolves the entity from the last segment of a `<prefix>.<schema>.<table>` topic and rejects unknown tables.
- `parse.py`: `parse_changes(records, entity)` turns landing records (the Kafka columns as landed, plus `landing_file`, the path of the landing file each record was read from) into typed bronze rows and quarantine rows. It is a pure DataFrame transform, so it works on batch and streaming DataFrames.
- `columns.py`: the names of the metadata and quarantine columns, each defined once and used everywhere through its constant.
- `descriptions.py`: the descriptions of the metadata and quarantine columns, and the table comments.
- `decimals.py` and `content_hash.py`: the decimal decoding and the content hash below, in plain Spark SQL functions (no Python UDFs).
- `job.py`: the bronze streaming job, which runs `parse_changes` on every landing topic folder and appends the results to Iceberg tables.

Run it with:

```sh
make spark-submit SPARK_APP=analytics/pipelines/src/pipelines/bronze/job.py SPARK_CONF="spark.cores.max=2" SPARK_UI_PORT=4041
```

- **Two streaming queries per topic** in `KAFKA_TOPICS`, for the entity the topic names: bronze rows are appended to `<ICEBERG_CATALOG>.bronze.<entity>` (checkpoint `<CHECKPOINT_ROOT>/bronze/<entity>/`), quarantine rows, plus `quarantined_at` (the micro-batch's processing time), to `<ICEBERG_CATALOG>.bronze.<entity>_quarantine` (checkpoint `<CHECKPOINT_ROOT>/bronze/<entity>_quarantine/`). Locally that is `lake.bronze.customers`, `lake.bronze.customers_quarantine` and so on.
- **Quarantine rows record their landing file:** the reader adds `landing_file` from the file source's `_metadata.file_path` column, so each quarantined record points at the Avro file it was read from. Bronze rows do not carry it.
- **Tables are created at start** when missing (`CREATE NAMESPACE` and `CREATE TABLE IF NOT EXISTS`), with the columns of the `parse_changes` output, partitioned by `days(published_at)`. `write.distribution-mode` is `none`, because streaming runs without adaptive execution and a hash distribution would shuffle every micro-batch into `spark.sql.shuffle.partitions` tasks. An existing table is never altered.
- **Every table and column is described:** the `CREATE TABLE` statement is generated with a table `COMMENT` and a `COMMENT` on every column, which Iceberg stores as the field's `doc`. Business columns take the `description` of their `SourceColumn` in `entities.py`; the metadata and quarantine columns and the table comments come from `descriptions.py`. The job refuses to create a table with a column that has no description, and a unit test checks every entity's tables, so a new column cannot be added undocumented. `DESCRIBE TABLE EXTENDED lake.bronze.customers` shows them.
- **Only committed landing files are read:** the file stream source follows the landing sink's `_spark_metadata` log, so files left by a failed landing micro-batch are ignored. The landing topic folders must exist when the job starts, so start it after the landing job has written to them.
- **Restart-safe, exactly once:** each query's checkpoint records the landing files it has processed, and Iceberg's streaming sink stores the query id and batch id in every snapshot and skips a batch it has already committed. Stopping and restarting the job neither skips nor duplicates rows: each `source_partition` and `source_offset` of a topic appears once.
- **Micro-batches** run every `BRONZE_TRIGGER_SECONDS` until the job is stopped (Ctrl+C); `SPARK_APP_ARGS=--available-now` processes what has landed so far and then stops.
- **Fail fast:** when any query stops or fails, the job stops the other queries and the Spark session, and exits non-zero if a query failed (an available-now run first lets the other queries finish), so a supervisor (such as Compose's `restart: unless-stopped`) or the operator restarts the whole job from its checkpoints.

| Entity | Key | Content columns | Audit columns |
| --- | --- | --- | --- |
| `customers` | `customer_id` | `customer_name`, `is_active`, `customer_address` | `updated_at`, `updated_by`, `created_at`, `created_by` |
| `products` | `product_id` | `product_name`, `barcode`, `unity_price`, `is_active` | same four |
| `orders` | `order_id` | `order_date`, `delivery_date`, `customer_id`, `status` | same four |
| `order_items` | `order_item_id` | `order_id`, `product_id`, `quantity` | same four |

Bronze columns and where they come from:

| Bronze column | Source | Type |
| --- | --- | --- |
| business columns | the row image: `before` for a delete, `after` otherwise; `quanity` is renamed to `quantity` | `TIMESTAMP(3)` → `timestamp` (`timestamp_millis`, wall clock read as UTC), `DATE` → `date` (`date_from_unix_date`), `DECIMAL` → `decimal(38,10)`, others as in the source |
| `record_source` | Kafka topic | string |
| `source_partition`, `source_offset` | Kafka partition and offset | int, long |
| `published_at` | Kafka record timestamp | timestamp |
| `change_op` | `op`: `c` → `create`, `u` → `update`, `d` → `delete`, `r` → `snapshot` | string |
| `commit_sequence` | `source.lsn` | long |
| `changed_at` | `source.ts_ms` (commit time in the source) | timestamp |
| `captured_at` | envelope `ts_ms` (when Debezium processed the change) | timestamp |
| `content_hash` | the content columns, see below | string (hex SHA-256) |

Kafka coordinates are lineage; the change identity is the entity key, `commit_sequence` and `change_op`. `unity_price` is decoded from its base64 two's-complement bytes with `try_to_binary`, `hex` and `conv`, then scaled by its `scale`; values up to eight bytes (a signed long) that fit `decimal(38,10)` exactly are decoded without loss.

**Tombstones** (records with a `null` value) are dropped: they only mark a key for log compaction, and landing keeps them. **Invalid records** do not fail the job; they become quarantine rows with `record_source`, `source_partition`, `source_offset`, `published_at`, `landing_file` (the landing file the record was read from), the raw `key` and `value` as strings, and `failure_reason`, the first failed check:

| `failure_reason` | When |
| --- | --- |
| `value is not a JSON Debezium envelope` | the value is not JSON, or lacks `op`, `ts_ms`, `source.lsn` or `source.ts_ms` |
| `unknown op` | `op` is not `c`, `u`, `d` or `r` (for example a truncate, `t`) |
| `missing key column: <column>` | the row image has no key value, or there is no row image |
| `missing NOT NULL column: <column>` | a column that is NOT NULL in the source DDL is null or absent |
| `undecodable or overflowing decimal: <column>` | not base64, empty, more than eight bytes, no scale, or not exactly representable as `decimal(38,10)` |

**`content_hash`** is the SHA-256 (`sha2(…, 256)`) of the entity's content columns, so silver can skip a version whose content did not change (re-snapshots, audit-only updates):

- each non-null content column contributes `name:length:value`, where `length` is the number of characters of `value`; parts are sorted by column name and joined with `|`;
- null columns are left out entirely; an empty string still contributes `name:0:`, so null and `''` differ;
- the key, the audit columns and the change metadata are excluded;
- values are formatted as: dates `yyyy-MM-dd`; timestamps in UTC as `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`, whatever the session time zone; decimals at the fixed scale of 10 (`1.5300000000`); booleans `true`/`false`; integers and strings as is.

**Schema evolution.** The schemas are explicit, so a source change never alters bronze silently:

| Source change | What happens | Automatic or manual |
| --- | --- | --- |
| Column added | Ignored until it is added to the `Entity`. Once added, older rows read it as null and keep their hash, because nulls are left out of `content_hash`. | Adding it to the `Entity` is manual; old hashes stay valid automatically. |
| Column removed | The field is absent from new events, so the column becomes null and drops out of the hash: silver records one new version per key. A removed NOT NULL column quarantines its events until it is no longer declared NOT NULL. | Automatic for nullable columns; manual for NOT NULL ones. |
| Column renamed | The old column becomes null and the new one is ignored until the `Entity` maps it (`renamed_to` keeps the bronze name stable). | Manual change to the `Entity`; the next event per key gets a new hash, so silver records one new version per key. |
| Type changed | Values that no longer parse are quarantined (NOT NULL columns, decimals) or read as null. | Manual change to the `Entity` and its conversion; one new version per key, as for a rename. |

**Tables created by an earlier version of the job.** `CREATE TABLE IF NOT EXISTS` leaves an existing table as it is, so column and table descriptions apply only to newly created tables. A local lake whose bronze tables predate them has no comments, and still has `change_position` where the job now writes `commit_sequence`, so appends to it fail. Drop those tables (`DROP TABLE lake.bronze.<entity> PURGE` in `make spark-sql`) together with their checkpoints under `<CHECKPOINT_ROOT>/bronze/`, so the job recreates them and reprocesses the landing zone; `make clean CONFIRM=1` does the same by discarding all local data. A table that is kept can get its descriptions by hand with `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`.

#### Tests, lint and format

The checks run in a throwaway container from the `app` image, with `analytics/` and `tests/` mounted, so they use the cluster's exact Spark and Python versions and pick up edits without a rebuild. They use their own Compose project, named after the checkout folder (`deel-checks-<folder>`, lowercased, with every character other than letters, digits, `_` and `-` replaced by `-`), with one app image (`<project>-spark-app`), so checks in different checkouts or worktrees never share an image and can run in parallel:

```sh
make test       # unit tests with branch coverage; fails below 90%
make lint       # ruff lint and format check
make format     # format the code and apply safe lint fixes (files keep your ownership)
make typecheck  # check types with ty; any diagnostic fails
```

- **Unit tests** live next to each project and mirror its source tree (`analytics/<project>/tests/test_<module>.py`).
- **Coverage** is measured on the `common` and `pipelines` packages with branch coverage. The 90% floor is configured in `pyproject.toml` (`[tool.coverage.report] fail_under`), and `make test` fails below it even when every test passes.
- **Lint and format** use [ruff](https://docs.astral.sh/ruff/), configured in `pyproject.toml` (`[tool.ruff]`).
- **Typing** is required for all code. ruff's `ANN` rules fail `make lint` when an annotation is missing, and [ty](https://docs.astral.sh/ty/) checks that the annotations are correct, including calls into PySpark, whose types it reads from the image (`[tool.ty]` in `pyproject.toml`). ty is in beta, so its version is pinned in `uv.lock`; it only runs in the container, where PySpark is available.

#### Infra tests

Infra tests check the platform itself rather than the pipeline code. They need Docker and [uv](https://docs.astral.sh/uv/) on the host, and run locally only (not in CI):

```sh
make test-infra   # about five minutes: building images, starting the stack and the Spark runs
```

They start an isolated Compose project named after the checkout folder (`deel-infra-tests-<folder>`, sanitised like the code-check project, with no host ports), so they never touch your running stack or its data, and runs from different worktrees can run in parallel. Each project builds one Spark image (`<project>-spark`, shared by the master and the workers) and one app image (`<project>-spark-app`); at the end the project is removed with its volumes and those two images. Two full stacks at once need enough memory for Docker, so very heavy runs may still be worth staggering. They submit the apps in `tests/infra/apps/` with `make spark-submit` and check that:

- the Iceberg smoke job writes catalog metadata to PostgreSQL and table files to SeaweedFS;
- executors write and read Avro files on `s3a://lake/` and write and read a Kafka topic with the bundled connectors;
- drivers and executors run the same Spark (4.1.4) and Python (3.14) versions;
- each application run gets its own driver container, which is removed when the application ends;
- two applications capped with `spark.cores.max=2` run at the same time, both with executors, in two separate driver containers;
- the landing example queries (`make spark-sql`) return the expected records from neutral, landing-shaped fixture data (topics `topic-a` and `topic-b`), including an update, a delete, a tombstone and a snapshot read, and inspect only the chosen topic;
- the bronze job, run with `--available-now` on records landed through the landing job's file sink (topics `test.operations.<table>`), writes typed rows with their change metadata to each entity's Iceberg table, partitioned by publish day; quarantines invalid records while valid ones flow, each with the path of the landing file it was read from, and drops tombstones; after new records land, a second run appends only those, and a third run with nothing new appends nothing, so every Kafka offset is written exactly once; and ignores an Avro file in a landing folder that the commit log does not record.

#### Pre-commit hooks

Install the hooks once per clone (needs [uv](https://docs.astral.sh/uv/) and Docker):

```sh
make hooks
```

Every `git commit` then runs, in order, stopping at the first failure:

| Hook | Runs when the commit touches | Where |
| --- | --- | --- |
| `uv.lock` matches `pyproject.toml` | any `pyproject.toml`, `uv.lock` | host |
| ruff check, ruff format check | Python files, `pyproject.toml` | host, with the ruff version from `uv.lock` |
| ty | `analytics/`, `tests/`, `pyproject.toml`, `uv.lock` | app container (`make typecheck`) |
| unit tests with the 90% coverage floor | `analytics/`, `tests/`, `pyproject.toml`, `uv.lock` | app container (`make test`) |

A commit that only changes documentation or configuration skips all of them. Commits that touch code take about 20–30 seconds longer, mostly for the container checks. Run `make format` to fix formatting before committing again. `git commit --no-verify` skips the hooks, which is why CI runs the same checks.

#### Continuous integration

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the code checks on every pull request to `main` (before merging) and on every push to `main` (after merging):

| Job | What it runs |
| --- | --- |
| `lint` | `uv lock --check`, `ruff check`, `ruff format --check` |
| `typecheck` | `make typecheck` in the app image |
| `test` | `make test` in the app image, with the 90% coverage floor |

The app image is built once per job with Docker's layer cache stored in GitHub Actions, and `make` reuses it (`APP_BUILD=`), so CI runs exactly the commands used locally. Infra tests are not part of CI; run them locally with `make test-infra`.
