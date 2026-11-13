# Silver

Silver is the version history of each source table: one row per bronze change, appended and never updated or removed. Gold reads it as a stream, and an Iceberg stream only accepts append snapshots, so silver must stay append-only. A change that bronze holds more than once (Debezium delivers at least once, and a replay delivers changes again) gets one row. Versions that change nothing, such as a re-snapshot or an update of audit columns only, keep rows of their own; the views `<entity>_history` and `<entity>_current` fold them away for readers. The package `analytics/pipelines/src/pipelines/lake/silver/` holds:

- `versions.py`: `versions(bronze, entity)` turns bronze rows into version rows. It is a pure DataFrame transform (no UDFs), so it works on batch and streaming DataFrames.
- `deduplication.py`: `drop_duplicate_changes(rows, entity)` keeps one row per change. It is separate from `versions` because it is stateful on a stream: it needs a checkpoint, and its tests feed several micro-batches.
- `columns.py`: the metadata columns silver copies from bronze and the one it adds, in table order.
- `sql/history.sql.j2` and `sql/current.sql.j2`: the SQL of the history and current views, as Jinja templates filled in with the entity's table, key and columns.
- `views.py`: `silver_views(entity, settings)`, the definitions of an entity's history and current views (their rendered SQL and their descriptions), and `refresh_views(spark, settings)`, which creates or updates the views of every entity.
- `views_job.py`: the one-shot job that runs `refresh_views`, then creates or updates the [gold views in the lake](gold/writer.md#gold-in-the-lake) (`make lake-views`).
- `job.py`: the silver streaming job, which creates its tables at start, then reads each bronze table as an Iceberg stream, turns its rows into versions, drops duplicate changes and appends the rest to the silver table, defined by `tables/silver_template.avsc` (partitioned by `_changed_at`) through `table_definition`, as in bronze.

Run it as a service with `make pipeline PIPELINE=silver` (see [Running the pipeline](../operations/running-the-pipeline.md)). It reads the bronze tables, so bronze must be running too (with landing before it), or use `PIPELINE=all`. Until the bronze job has created an entity's table, the silver job stops at start with `TABLE_OR_VIEW_NOT_FOUND` for that table, before it creates any silver table, and Compose restarts it. Don't also start it with `make spark-submit` while the service runs: both would write to the same tables with the same checkpoints.

- **One streaming query per topic** in `KAFKA_TOPICS`, for the entity the topic names (the same configuration and mapping as bronze): it reads `<ICEBERG_CATALOG>.bronze.<entity>` with Iceberg's streaming source (`spark.readStream.table`) and appends to `<ICEBERG_CATALOG>.silver.<entity>` (checkpoint `<CHECKPOINT_ROOT>/silver/<entity>/`). Locally that is `lake.bronze.customers` into `lake.silver.customers` and so on.
- **Columns:** the entity key and the business and audit columns, exactly as bronze has them (under their lake names, so `quantity`, and `unity_price` as `decimal(38,10)`); then `_commit_sequence`, `_change_op`, `_changed_at` and `_content_hash`, copied from bronze; then `_is_deleted`, true when `_change_op` is `delete`. Bronze takes a delete's values from Debezium's `before` image, which holds the full row because the source tables use `REPLICA IDENTITY FULL`, so a delete version keeps the row's last known values. The Kafka coordinates, `_published_at` and `_captured_at` stay in bronze.
- **Tables are created at start** when missing, the same way as bronze (`CREATE NAMESPACE IF NOT EXISTS silver`, then `writeTo(...).create()` from an empty DataFrame with the schema of the version rows), partitioned by `days(_changed_at)`, with `write.distribution-mode` `none` for the same reason as bronze. Every column is nullable. The schema of an existing table is never altered.
- **Every table and column is described from the Avro schemas,** as in bronze: the table comment is the description in `tables/silver_template.avsc`, the business columns take the `doc` of their source schema and the metadata columns the one in the template, which defines the four copied columns exactly as `bronze_template.avsc` does. On every start the job updates only the stored descriptions that differ (`ALTER TABLE ... SET TBLPROPERTIES ('comment' = ...)`, `ALTER TABLE ... ALTER COLUMN ... COMMENT ...`), and it refuses to start when a column has no definition or a different type. `make schemas` renders the flat result to `target/schemas/silver/`, and `DESCRIBE TABLE EXTENDED lake.silver.customers` shows the stored descriptions.
- **Duplicate changes are dropped:** a change is identified by the entity key, `_commit_sequence` and `_change_op`. The Kafka offset and `_captured_at` are not part of it, because Debezium sends a change again at a new offset after a restart. Each query runs `dropDuplicates` on those three columns, so a change that bronze holds twice, in the same micro-batch or in different ones, gets one version row. There is no watermark: a late change, older than versions already stored, is appended like any other and never dropped, and the state keeps every change it has seen (one small entry per change) without ever removing one. Different keys can share a `_commit_sequence` (every row of a snapshot has the snapshot's position), and each of them is kept.
- **Unchanged versions are kept:** a re-snapshot, an update of audit columns only, a delete (which keeps the last known values) and a re-create with the same values each get a row, even when its `_content_hash` equals the previous version's. The history view folds the versions that change nothing (see the views below), so "one version per change that changed the row" holds for the view, not the table, and the table has more rows than the view.
- **Deduplication state in RocksDB:** the job sets `spark.sql.streaming.stateStore.providerClass` to `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` on its own Spark session, so the state lives in RocksDB on the executors' local disks instead of in executor memory, and each micro-batch saves it under `<CHECKPOINT_ROOT>/silver/<entity>/state/`. It turns on RocksDB changelog checkpointing (`spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled`), so a batch uploads only its state changes (`<version>.changelog`) and RocksDB snapshots (`<version>.zip`) are written in the background; Spark switches an existing checkpoint to the changelog, and back, on the next start. It also sets `spark.sql.shuffle.partitions` to 2, one state partition per core the job gets, instead of Spark's 200, which would save 200 state stores per entity on every micro-batch. The other jobs keep Spark's defaults. A query keeps the provider and the number of state partitions its checkpoint was created with, so changing either needs the reset below.
- **Restart-safe, exactly once:** each query's checkpoint records the bronze snapshots it has read and the deduplication state, and Iceberg's streaming sink skips a batch it has already committed, so a restart neither skips nor repeats bronze rows, and a change seen before the restart is still dropped after it. Bronze keeps every delivery: a change Debezium delivered twice is two rows in bronze and one in silver.
- **Micro-batches** run every `SILVER_TRIGGER_SECONDS` until the job is stopped; `SPARK_APP_ARGS=--available-now` processes the bronze snapshots committed so far and then stops. An `--available-now` restart goes back to the first bronze snapshot its checkpoint recorded, so once lake maintenance has expired that snapshot it fails, and needs the reset below; the service, on its processing-time trigger, is not affected.
- **Fail fast:** like bronze, when any query stops or fails the job stops the others and exits non-zero if a query failed, so Compose restarts it from its checkpoints.

**Views: version history and current state.** Two Iceberg views per entity sit next to the silver table, in the same catalog and namespace (the `lake` catalog stores views because it is a JDBC catalog with schema version V1). They read the table by its fully qualified name. Their SQL is in `silver/sql/history.sql.j2` and `silver/sql/current.sql.j2`, filled in with the entity's table, key and columns.

The streaming job does not manage them. `refresh_views(spark, settings)` in `pipelines/lake/silver/views.py` creates them and updates them, and lake maintenance (see [Lake maintenance](../operations/maintenance-and-recovery.md#lake-maintenance)) and two commands run it:

- **`make lake-views`** runs it once, as the one-shot job `silver/views_job.py`, against the running stack. Run it after changing a view's SQL or its descriptions. It prints how many silver views and how many gold views it created or replaced; 0 means all were up to date. While the gold tables do not exist yet, it prints `GOLD VIEWS skipped: ...` instead: the gold job creates them, and their views, when it starts.
- **`make pipeline PIPELINE=silver`** (or `all`) also starts the `lake-views` service, which runs the same job once. On a fresh stack the silver tables do not exist yet, so it fails with `MissingSilverTablesError`, before creating any view, and Compose restarts it (`restart: on-failure`) until the silver job has created every table. Then it creates the views and exits.

In production, CI would run the same command (`make lake-views`, or the job on the production cluster) whenever a view definition changes.

What the views compute:

- **The fold rule:** per key, ordered by `_commit_sequence`, a version folds into the previous one when its `_content_hash` and its `_is_deleted` both equal the previous version's (`lag`); the first version of a key never folds. So a re-snapshot and an audit-only update fold, while a delete, which keeps the last known values and so the previous hash, does not, and neither does a re-create after it with the same values.
- **`<entity>_history`** (`lake.silver.customers_history`, ...) is the version history: the folded versions are filtered out first, then each remaining version gets `_valid_from` (its `_changed_at`), `_valid_to` (the next remaining version's `_valid_from`, null for the latest) and `_is_current` (whether it is the key's latest remaining version). Columns: the key, the business and audit columns, `_commit_sequence`, `_change_op`, `_changed_at`, `_content_hash`, `_is_deleted`, `_valid_from`, `_valid_to`, `_is_current`. A delete is a version of its own: it ends the validity of the version before it, and it is the current version while the key stays deleted.
- **`<entity>_current`** is the current state: one row per live key, the rows of `<entity>_history` with `_is_current` true and `_is_deleted` false, so a key whose latest version is a delete is left out. It reads the history view, so the fold rule is written once. Columns: the same as the history view without `_valid_to`, `_is_current` and `_is_deleted`.
- **Late versions need no special handling:** the views compute over the whole table on every query, so a version appended after newer ones takes its place by `_commit_sequence`: it is valid until the next version, the version before it now ends at it, and the current state does not change.
- **Described from the Avro schemas,** in `tables/silver_history_template.avsc` and `tables/silver_current_template.avsc`, through `CREATE VIEW ... (<column> COMMENT ..., ...) COMMENT ... AS ...`. `_valid_from`, `_valid_to`, `_is_current` and `_is_deleted` have the same definitions as in gold. `make schemas` renders them to `target/schemas/silver/<entity>_history.avsc` and `<entity>_current.avsc`.
- **Replaced only when they change:** `refresh_views` reads each stored view (its query from `SHOW CREATE TABLE`, its comments from `DESCRIBE TABLE EXTENDED`) and runs `CREATE OR REPLACE VIEW` only when the query or a description differs from the one it builds, so running it again does not write a new view version.
- **Gold applies the same rule:** gold streams the silver table, not the views, so its writer folds versions with the same rule; the silver → gold contract is in [Inputs from silver](gold/model.md#inputs-from-silver).

Query them in `make spark-sql`:

```sql
-- Every version of customer 7, oldest first, with the time range it was current
SELECT customer_id, customer_name, _change_op, _is_deleted, _valid_from, _valid_to, _is_current
FROM lake.silver.customers_history
WHERE customer_id = 7
ORDER BY _commit_sequence;

-- The orders as they were at a point in time
SELECT order_id, status
FROM lake.silver.orders_history
WHERE _valid_from <= TIMESTAMP '2026-11-08 15:00:00'
  AND (_valid_to IS NULL OR _valid_to > TIMESTAMP '2026-11-08 15:00:00')
  AND NOT _is_deleted;

-- The live orders by status
SELECT status, count(*) AS orders FROM lake.silver.orders_current GROUP BY status;
```

**Tables and checkpoints written before deduplication.** Deduplication adds state to every silver query, and Spark cannot add state to a query whose checkpoint was written without it: the job starts, but fails at the first micro-batch with new bronze rows (`CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE`). Those tables can also hold duplicate changes. Reset silver, and only silver:

1. Stop the job: `make pipeline-stop PIPELINE=silver`.
2. Drop the four tables in `make spark-sql`: `DROP TABLE lake.silver.customers PURGE;`, then the same for `lake.silver.products`, `lake.silver.orders` and `lake.silver.order_items`.
3. Delete the silver checkpoints, `<CHECKPOINT_ROOT>/silver/`. Locally: `curl -X DELETE "http://localhost:8888/buckets/lake/checkpoints/silver/?recursive=true"`.
4. Start the job again: `make pipeline PIPELINE=silver`. It creates the tables and versions every bronze row from the start.

Bronze, its tables and its checkpoints are not touched, and the views need nothing: they read the tables by name. `make clean CONFIRM=1` also works, by discarding all local data.
