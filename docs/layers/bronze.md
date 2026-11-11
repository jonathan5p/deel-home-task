# Bronze

Bronze is a typed, faithful log of every change event: one row per event, no deduplication. The package `analytics/pipelines/src/pipelines/bronze/` holds:

- `entities.py`: one `Entity` per source table, built from its Avro schema in `pipelines/schemas/sources/`, with its key, content and audit columns, its NOT NULL columns and the JSON row schema Debezium emits. `entity_for_topic` resolves the entity from the last segment of a `<prefix>.<schema>.<table>` topic and rejects unknown tables.
- `parse.py`: `parse_changes(records, entity)` turns landing records (the Kafka columns as landed, plus `_landing_file`, the path of the landing file each record was read from) into typed bronze rows and quarantine rows. It is a pure DataFrame transform, so it works on batch and streaming DataFrames.
- `columns.py`: the names of the raw Kafka columns quarantine keeps (`RawColumn`) and the column order of bronze and quarantine metadata, as string enums and tuples used instead of literals. The metadata column names themselves (`MetadataColumn`) and the `_change_op` values (`ChangeOp`) are in `pipelines/lake/columns.py`, shared with silver.
- `decimals.py` and `content_hash.py`: the decimal decoding and the content hash below, in plain Spark SQL functions (no Python UDFs).
- `job.py`: the bronze streaming job, which runs `parse_changes` on every landing topic folder and appends the results to Iceberg tables. Starting the queries and the fail-fast wait below are shared with silver, in `pipelines/lake/streams.py`.

The bronze and quarantine tables are defined by their templates (`tables/bronze_template.avsc` and `tables/bronze_quarantine_template.avsc`, both partitioned by `_published_at`). `table_definition` in `pipelines/lake/tables.py` builds any layer's table from its template, and the same module creates the Iceberg tables and updates their descriptions for every layer.

Run it as a service with `make pipeline PIPELINE=bronze` (see [Running the pipeline](../operations/running-the-pipeline.md)). It reads what the landing job writes, so run both, or `PIPELINE=all`. Until the landing job has written a topic's first file, the bronze job stops with `PATH_NOT_FOUND` for that topic's folder and Compose restarts it. Don't also start it with `make spark-submit` while the service runs: both would write to the same tables with the same checkpoints.

- **Two streaming queries per topic** in `KAFKA_TOPICS`, for the entity the topic names: bronze rows are appended to `<ICEBERG_CATALOG>.bronze.<entity>` (checkpoint `<CHECKPOINT_ROOT>/bronze/<entity>/`), quarantine rows, plus `_quarantined_at` (the micro-batch's processing time), to `<ICEBERG_CATALOG>.bronze.<entity>_quarantine` (checkpoint `<CHECKPOINT_ROOT>/bronze/<entity>_quarantine/`). Locally that is `lake.bronze.customers`, `lake.bronze.customers_quarantine` and so on.
- **Quarantine rows record their landing file:** the reader adds `_landing_file` from the file source's `_metadata.file_path` column, so each quarantined record points at the Avro file it was read from. Bronze rows do not carry it.
- **Tables are created at start** when missing (`spark.catalog.tableExists`): `CREATE NAMESPACE IF NOT EXISTS`, then `writeTo(...).create()` from an empty DataFrame with the schema of the `parse_changes` output, partitioned by `days(_published_at)`. `write.distribution-mode` is `none`, because streaming runs without adaptive execution and a hash distribution would shuffle every micro-batch into `spark.sql.shuffle.partitions` tasks. Every column is nullable. The schema of an existing table is never altered (see schema evolution below).
- **Every table and column is described from the Avro schemas:** each field of the created schema carries its definition's description as its `comment`, which Iceberg stores as the field's `doc`, and the `comment` table property is the table description from `tables/bronze_template.avsc` or `tables/bronze_quarantine_template.avsc`. Business columns take the `doc` in their source schema (under their lake name, so `quantity`), the metadata and quarantine columns the one in their template's `fields`. `make schemas` renders the flat result to `target/schemas/bronze/`. The job refuses to start when a column has no definition or its Spark type differs from its definition's type (checked for every table before any is created), and a unit test checks every entity's tables. `DESCRIBE TABLE EXTENDED lake.bronze.customers` shows them.
- **Descriptions are kept in sync:** on every start the job compares the stored table comment and column docs of each existing table with the definitions and runs `ALTER TABLE ... SET TBLPROPERTIES ('comment' = ...)` and `ALTER TABLE ... ALTER COLUMN ... COMMENT ...` only for those that differ, so an unchanged table gets no new metadata version. To change a description, edit the schema file and restart the job.
- **Only committed landing files are read:** the file stream source follows the landing sink's `_spark_metadata` log, so files left by a failed landing micro-batch are ignored. The landing topic folders must exist when the job starts, so start it after the landing job has written to them.
- **Restart-safe, exactly once:** each query's checkpoint records the landing files it has processed, and Iceberg's streaming sink stores the query id and batch id in every snapshot and skips a batch it has already committed. Stopping and restarting the job neither skips nor duplicates rows: each `_source_partition` and `_source_offset` of a topic appears once.
- **Micro-batches** run every `BRONZE_TRIGGER_SECONDS` until the job is stopped (Ctrl+C); `SPARK_APP_ARGS=--available-now` processes what has landed so far and then stops.
- **Fail fast:** when any query stops or fails, the job stops the other queries and the Spark session, and exits non-zero if a query failed (an available-now run first lets the other queries finish), so a supervisor (such as Compose's `restart: unless-stopped`) or the operator restarts the whole job from its checkpoints.

| Entity | Key | Content columns | Audit columns |
| --- | --- | --- | --- |
| `customers` | `customer_id` | `customer_name`, `is_active`, `customer_address` | `updated_at`, `updated_by`, `created_at`, `created_by` |
| `products` | `product_id` | `product_name`, `barcode`, `unity_price`, `is_active` | same four |
| `orders` | `order_id` | `order_date`, `delivery_date`, `customer_id`, `status` | same four |
| `order_items` | `order_item_id` | `order_id`, `product_id`, `quantity` | same four |

Columns the pipeline adds start with `_` (for example `_commit_sequence`), so they never clash with source columns and read as metadata; business columns keep their source names.

Bronze columns and where they come from:

| Bronze column | Source | Type |
| --- | --- | --- |
| business columns | the row image: `before` for a delete, `after` otherwise; `quanity` is renamed to `quantity` | `TIMESTAMP(3)` → `timestamp` (`timestamp_millis`, wall clock read as UTC), `DATE` → `date` (`date_from_unix_date`), `DECIMAL` → `decimal(38,10)`, others as in the source |
| `_record_source` | Kafka topic | string |
| `_source_partition`, `_source_offset` | Kafka partition and offset | int, long |
| `_published_at` | Kafka record timestamp | timestamp |
| `_change_op` | `op`: `c` → `create`, `u` → `update`, `d` → `delete`, `r` → `snapshot` | string |
| `_commit_sequence` | `source.lsn` | long |
| `_changed_at` | `source.ts_ms` (commit time in the source) | timestamp |
| `_captured_at` | envelope `ts_ms` (when Debezium processed the change) | timestamp |
| `_content_hash` | the content columns, see below | string (hex SHA-256) |

Kafka coordinates are lineage; the change identity is the entity key, `_commit_sequence` and `_change_op`. `unity_price` is decoded from its base64 two's-complement bytes with `try_to_binary`, `hex` and `conv`, then scaled by its `scale`; values up to eight bytes (a signed long) that fit `decimal(38,10)` exactly are decoded without loss.

**Tombstones** (records with a `null` value) are dropped: they only mark a key for log compaction, and landing keeps them. **Invalid records** do not fail the job; they become quarantine rows with `_record_source`, `_source_partition`, `_source_offset`, `_published_at`, `_landing_file` (the landing file the record was read from), the raw `key` and `value` as strings, and `_failure_reason`, the first failed check:

| `_failure_reason` | When |
| --- | --- |
| `value is not a JSON Debezium envelope` | the value is not JSON, or lacks `op`, `ts_ms`, `source.lsn` or `source.ts_ms` |
| `unknown op` | `op` is not `c`, `u`, `d` or `r` (for example a truncate, `t`) |
| `missing key column: <column>` | the row image has no key value, or there is no row image |
| `missing NOT NULL column: <column>` | a column that is NOT NULL in the source DDL is null or absent |
| `undecodable or overflowing decimal: <column>` | not base64, empty, more than eight bytes, no scale, or not exactly representable as `decimal(38,10)` |

**`_content_hash`** is the SHA-256 (`sha2(…, 256)`) of the entity's content columns, so silver can skip a version whose content did not change (re-snapshots, audit-only updates):

- each non-null content column contributes `name:length:value`, where `length` is the number of characters of `value`; parts are sorted by column name and joined with `|`;
- null columns are left out entirely; an empty string still contributes `name:0:`, so null and `''` differ;
- the key, the audit columns and the change metadata are excluded;
- values are formatted as: dates `yyyy-MM-dd`; timestamps in UTC as `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`, whatever the session time zone; decimals at the fixed scale of 10 (`1.5300000000`); booleans `true`/`false`; integers and strings as is.

**Schema evolution.** The schemas are explicit, so a source change never alters bronze silently:

| Source change | What happens | Automatic or manual |
| --- | --- | --- |
| Column added | Ignored until it is added to the source table's schema (`sources/<table>.avsc`). Once added, older rows read it as null and keep their hash, because nulls are left out of `_content_hash`; the table needs the column too (`ALTER TABLE ... ADD COLUMN`, the job does not add it). | Adding it to the schema and the table is manual; old hashes stay valid automatically. |
| Column removed | The field is absent from new events, so the column becomes null and drops out of the hash: silver records one new version per key. A removed NOT NULL column quarantines its events until it is no longer declared NOT NULL. | Automatic for nullable columns; manual for NOT NULL ones. |
| Column renamed | The old column becomes null and the new one is ignored until the source schema maps it (`renamed_to` keeps the bronze name stable). | Manual change to the source schema; the next event per key gets a new hash, so silver records one new version per key. |
| Type changed | Values that no longer parse are quarantined (NOT NULL columns, decimals) or read as null. | Manual change to the source schema and its conversion; one new version per key, as for a rename. |

**Tables created by an earlier version of the job.** A local lake whose bronze tables predate the `_` prefix still has `change_position` and unprefixed metadata columns where the job now writes `_commit_sequence` and the other `_` columns, so appends to it fail. Drop those tables (`DROP TABLE lake.bronze.<entity> PURGE` in `make spark-sql`) together with their checkpoints under `<CHECKPOINT_ROOT>/bronze/`, so the job recreates them and reprocesses the landing zone; `make clean CONFIRM=1` does the same by discarding all local data. Tables with the current columns only need a restart of the job to get their descriptions.
