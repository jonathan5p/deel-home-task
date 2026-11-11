# Learnings

Local-only facts, gotchas, and evidence that matter for building the pipeline and for writing M6 (architecture, sizing, known issues, evidence). Not tracked in GitHub.

Mark each item with how it is known: **verified** (read in source or observed at runtime), **reported** (from a helper's mapping, not re-checked), or **external** (vendor docs). Re-check reported items before citing them in the README.

## Source data and CDC format

- **verified**: Topics are `finance_db.operations.{customers,products,orders,order_items}`. The connector is `finance-db-connector`, using pgoutput and slot `cdc_pgoutput`, with a heartbeat every 10s.
- **verified**: JsonConverter runs with `schemas.enable=false` for key and value, so messages carry no embedded schema. Spark must declare all 4 schemas explicitly.
- **verified**: All 4 tables use `REPLICA IDENTITY FULL`, so `before` is populated on updates.
- **verified**: Column `order_items.quanity` is misspelled in the DDL. The diagram says `quantity`.
- **verified**: Order statuses are inserted as `PENDING` and updated to one of `PROCESSING`, `COMPLETED`, `REPROCESSING`. Working definition: an open order is any status other than `COMPLETED`.
- **reported**: There are no foreign-key constraints; the relations exist only in the ERD. `order_items` has no price, so order value needs the product price as of the order time, which requires SCD2 product history.
- **reported**: `products.unity_price` is an unconstrained `DECIMAL`. With default `decimal.handling.mode=precise`, Debezium emits it as `{"scale": int, "value": "<base64 unscaled bytes>"}`. Prices pass through FLOAT before being stored.
- **reported**: `TIMESTAMP(3)` arrives as epoch milliseconds (no timezone) and `DATE` as days since epoch. Use `source.lsn` / `ts_ms` for ordering and deduplication.
- **reported**: No DELETEs are generated today. Debezium would still emit `op=d` plus a null-value tombstone, so readers must tolerate null values.
- **reported**: The pg_cron generators run `update_customers(5)` and `update_products(10)` every 2 min and `generate_orders(100)` every 1 min. Each order insert adds about 26 items.
- **reported**: Generator bugs to list as known source issues (not to fix):
  - Loops use `<=`, so each runs one extra iteration.
  - Items can attach to an earlier COMPLETED order: the re-select filters by customer and date only.
  - `quantity` can be 0.
  - Customers and products become mostly updates once their fixed name pools are exhausted.
- **reported**: The stack has a single Kafka broker with replication factor 1 and auto-created topics. The pre-created replication slot retains WAL even when the connector is down. `${PWD}` mounts break if Compose runs from another directory.

## Spark / Iceberg

- **external**: A Spark Structured Streaming read from an Iceberg table supports only append snapshots. Overwrite or delete snapshots (MERGE, UPDATE, DELETE) fail the stream unless skipped with `streaming-skip-overwrite-snapshots` / `streaming-skip-delete-snapshots`, which then silently skip those changes. A layer that feeds a downstream stream must be append-only (for example a versioned change log, with current state derived in views). This is why landing must stay append-only, and why a future silver layer would be modeled as an append-only versioned log. Label: L-ICEBERG-STREAM.
- **external**: The Spark Iceberg streaming sink records the query ID and epoch in snapshot metadata. With checkpoints, appends are exactly-once across restarts. Verify this in issue 3.2 rather than assuming it.
- **external**: Frequent micro-batch commits create many small files. Compaction (`rewrite_data_files`) and `expire_snapshots` are part of operating the landing table.

## Gold store (Postgres vs ClickHouse)

- **external**: Postgres `REFRESH MATERIALIZED VIEW` (even `CONCURRENTLY`) recomputes the whole view, so it is not suited to refreshing per micro-batch.
- **external**: ClickHouse ReplacingMergeTree deduplicates only during background merges. Correct current-state reads need `FINAL` or `argMax`. It suits large-scale OLAP, but makes "consistent results" harder. ClickHouse can also consume Kafka directly (Kafka engine). Do not use that here: Spark must remain the processor being evaluated.

## Conduktor monitor

- **external/verified**: Console requires an external PostgreSQL 13+ and has no embedded or SQLite fallback. Cortex is optional: it provides only metrics history, dashboards and alerting.
- **verified**: Console refuses to start if the admin password lacks 8+ characters with an uppercase letter, a lowercase letter, a digit and a symbol. The error appears only in the container logs.
- **verified**: Recreating Console is required after env changes. A running container kept the stale `CDK_MONITORING_*` variables after Cortex was removed from Compose.

## Environment

- **verified**: Docker Desktop on this machine is reached at `DOCKER_HOST=unix://$HOME/.docker/desktop/docker.sock`. Compose v2 (`docker compose`) is available there.
- **verified**: The repository's GitHub remote is `jonathan5p/deel-home-task` (hyphens), with private project 2 "Deel Home Task".

## Assignment notes that shape M6

- The architecture diagram must include all components, the Spark cluster sizing strategy, and the variables behind each decision. Key Spark metrics earn bonus points.
- Evidence can be data screenshots, Spark UI screenshots, and similar.
- Keep the full commit history: merge PRs with merge commits, not squash.
- The README must be detailed enough to run the application. Ideas not implemented should be recorded (M7 backlog → README next steps).

## Analytics PostgreSQL (verified 2026-11-05, issue #7)
- **Verified:** `postgres:18.x` images set `PGDATA=/var/lib/postgresql/18/docker`. Mount the named volume at `/var/lib/postgresql`, not the pre-18 `/var/lib/postgresql/data`, so major upgrades (pg_upgrade --link) can see both data dirs.
- **Verified:** `docker compose down -v <service>` (Compose 2.x, Docker 28.4) removes only that service's container and its named volumes; other services and volumes stay. Used for the analytics-only reset documented in the README.
- **Verified:** init scripts in `/docker-entrypoint-initdb.d` run only on an empty data dir; restarts log "Skipping initialization".
- **Testing pattern:** validate stack changes in an isolated Compose project (`COMPOSE_PROJECT_NAME=analytics-check`) and tear it down with `--profile '*' down -v`, so the user's real volumes are never touched.

## SeaweedFS and Iceberg catalog (verified 2026-11-05, issue #10)
- **Verified:** `weed mini` (SeaweedFS 4.52) creates an admin S3 identity from `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY` env ("Added admin identity from AWS environment variables"); wrong or missing keys get 403. `-bucket=<name>` creates the bucket on startup ("created bucket lake" / "bucket lake already exists"), about a second after the S3 port opens, so health must check the bucket, not the port. `wget` and `curl` exist in the image; curl `--aws-sigv4 "aws:amz:us-east-1:s3"` is a handy S3 client.
- **Verified:** `weed mini` binds its admin UI to 127.0.0.1 when no admin password is set; the filer UI (8888) is enough to browse files. It also starts an Iceberg REST catalog on 8181.
- **Verified:** Iceberg's JdbcCatalog does **not** resolve `env:VAR` in `jdbc.user` (it sent the literal string). Spark's own `${env:VAR}` substitution in `spark-defaults.conf`/`--conf` works for `spark.sql.catalog.*` options.
- **Verified:** with credentials resolved on the driver, executors on workers without those env vars still write to S3 (S3FileIO is serialized with its properties).
- **Verified:** Iceberg JDBC catalog creates `iceberg_tables` and `iceberg_namespace_properties` in `public` of the target database on first use.
- **Testing pattern:** when the user's stack is running, isolate tests with `COMPOSE_PROJECT_NAME=<x>` plus `COMPOSE_FILE=docker-compose.yaml:<scratch override>` that resets host ports (`ports: !reset []`) and retags locally built images, so neither ports nor the user's image tags are touched. zsh does not word-split `$DC` variables; use `COMPOSE_FILE` instead.

## Spark connectors: Kafka, Avro, S3A (verified 2026-11-08, issue #20/#21)
- **Verified:** Spark 4.1.4's Kafka connector needs `spark-sql-kafka-0-10_2.13`, `spark-token-provider-kafka-0-10_2.13` (4.1.4), `kafka-clients` 3.9.1 and `commons-pool2` 2.12.1; the image already has lz4, snappy, zstd-jni, scala-parallel-collections and jsr305.
- **Verified:** Spark 4.1.4 ships Hadoop 3.4.2 (`hadoop-client-api/runtime`). `hadoop-aws` 3.4.2 works with the AWS SDK 2.54.17 classes inside `iceberg-aws-bundle-1.12.0` plus two SDK modules at the same version: `s3-transfer-manager` and `apache-client` (it fails with `ClassNotFoundException: software.amazon.awssdk.http.apache.ApacheHttpClient` without the latter). Spark already ships httpclient 4.5.14/httpcore 4.4.16. About 11 MB in total instead of the ~600 MB SDK bundle.
- **Verified:** Spark substitutes `${env:VAR}` for `spark.sql.catalog.*` settings but NOT for `spark.hadoop.*`: S3A received the literal text and SeaweedFS answered 403. `make spark-submit` now passes `spark.hadoop.fs.s3a.access.key/secret.key` from the driver container env; executors receive them through SparkConf. They appear as `*********(redacted)` in event logs (zstd-compressed by default in Spark 4).
- **Verified:** Spark 4 runs with ANSI mode on: `CAST(bigint AS BINARY)` is rejected.
- **Verified:** building the same image tag from three services in one `docker compose up --build` can fail the first time; build once (`docker compose build spark-master`) before `up`.
- **Verified:** Kafka in Compose has no health check; wait for `kafka-topics --list` to succeed before using it (infra `Stack.up`).
