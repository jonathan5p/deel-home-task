# Bronze

Bronze is a typed, faithful log of every change event: one row per event, no deduplication. The code is in `analytics/pipelines/src/pipelines/lake/bronze/`.

Run it with `make pipeline PIPELINE=bronze` (or `PIPELINE=all`, since it reads what landing writes; see [Running the pipeline](../operations/running-the-pipeline.md)). Until landing has written a topic's first file, the job stops with `PATH_NOT_FOUND` and Compose restarts it. Do not also start it with `make spark-submit`: both would write the same tables with the same checkpoints.

## How it runs

| Behaviour | What it means |
| --- | --- |
| Two queries per topic | Valid rows go to `lake.bronze.<entity>`, invalid ones to `lake.bronze.<entity>_quarantine`, each with its own checkpoint under `<CHECKPOINT_ROOT>/bronze/` |
| Reads only committed landing files | The file source follows landing's `_spark_metadata` log, so files of a failed landing batch are ignored |
| Exactly once | Checkpoints record the files processed, and Iceberg skips a batch it already committed: each Kafka offset appears once |
| Tables created at start | Partitioned by `days(_published_at)`, write distribution `none`, every column nullable; an existing table's schema is never altered |
| Described from the Avro schemas | Table and column descriptions come from `tables/bronze_template.avsc` and the source schemas, and are updated on start when they differ; the job refuses to start when a column has no definition or a different type |
| Trigger | Every `BRONZE_TRIGGER_SECONDS`; `SPARK_APP_ARGS=--available-now` processes what has landed and stops |
| Fail fast | When one query fails, the job stops the others and exits non-zero, and Compose restarts it from its checkpoints |

## Columns

| Entity | Key | Content columns |
| --- | --- | --- |
| `customers` | `customer_id` | `customer_name`, `is_active`, `customer_address` |
| `products` | `product_id` | `product_name`, `barcode`, `unity_price`, `is_active` |
| `orders` | `order_id` | `order_date`, `delivery_date`, `customer_id`, `status` |
| `order_items` | `order_item_id` | `order_id`, `product_id`, `quantity` |

Every entity also has the audit columns `updated_at`, `updated_by`, `created_at` and `created_by`. Columns the pipeline adds start with `_`.

| Bronze column | Source | Type |
| --- | --- | --- |
| Business columns | The row image: `before` for a delete, `after` otherwise; `quanity` renamed to `quantity` | Timestamps as UTC `timestamp`, dates as `date`, decimals as `decimal(38,10)` |
| `_record_source` | Kafka topic | string |
| `_source_partition`, `_source_offset` | Kafka partition and offset | int, long |
| `_published_at` | Kafka record timestamp | timestamp |
| `_change_op` | `op`: `c` → `create`, `u` → `update`, `d` → `delete`, `r` → `snapshot` | string |
| `_commit_sequence` | `source.lsn` | long |
| `_changed_at` | `source.ts_ms`, the commit time in the source | timestamp |
| `_captured_at` | Envelope `ts_ms`, when Debezium processed the change | timestamp |
| `_content_hash` | SHA-256 of the content columns | string |

The change identity is the entity key, `_commit_sequence` and `_change_op`; the Kafka coordinates are lineage. `unity_price` arrives as Debezium's base64 variable-scale decimal and is decoded in plain Spark SQL, without loss for values that fit `decimal(38,10)`.

## Quarantine

Tombstones (records with a null value) are dropped; landing keeps them. Invalid records do not fail the job: they go to quarantine with their Kafka coordinates, their landing file (`_landing_file`), the raw key and value, and `_failure_reason`, the first check that failed:

| `_failure_reason` | When |
| --- | --- |
| `value is not a JSON Debezium envelope` | Not JSON, or `op`, `ts_ms`, `source.lsn` or `source.ts_ms` missing |
| `unknown op` | `op` is not `c`, `u`, `d` or `r` (for example a truncate) |
| `missing key column: <column>` | No key value, or no row image |
| `missing NOT NULL column: <column>` | A column that is NOT NULL in the source is null or absent |
| `undecodable or overflowing decimal: <column>` | Not base64, empty, over eight bytes, no scale, or not exact in `decimal(38,10)` |

## Content hash

`_content_hash` lets silver recognise versions that change nothing, such as re-snapshots and audit-only updates.

- Each non-null content column contributes `name:length:value`; the parts are sorted by name and joined with `|`.
- Null columns are left out, so adding a nullable column keeps the old hashes; an empty string still counts, so null and `''` differ.
- The key, the audit columns and the change metadata are excluded.
- Values are formatted the same way in every session: dates `yyyy-MM-dd`, timestamps in UTC with milliseconds, decimals at scale 10, booleans `true`/`false`.

## Schema evolution

The schemas are explicit, so a source change never alters bronze silently.

| Source change | What happens | What I do |
| --- | --- | --- |
| Column added | Ignored until added to `sources/<table>.avsc`; old rows read it as null and keep their hash | Add it to the schema and `ALTER TABLE ... ADD COLUMN` |
| Column removed | Becomes null, so silver records one new version per key; a NOT NULL column quarantines its events | Nothing for nullable columns; drop the NOT NULL flag otherwise |
| Column renamed | The old column becomes null and the new one is ignored | Map it in the source schema (`renamed_to` keeps the bronze name) |
| Type changed | Values that no longer parse are quarantined or read as null | Change the source schema and its conversion |
