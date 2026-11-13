# Silver

Silver is the version history of each source table: one row per change, appended and never updated. Gold reads it as a stream, and an Iceberg stream accepts only append snapshots, so silver stays append-only. The code is in `analytics/pipelines/src/pipelines/lake/silver/`.

Run it with `make pipeline PIPELINE=silver` (or `PIPELINE=all`; see [Running the pipeline](../operations/running-the-pipeline.md)). Until bronze has created an entity's table, the job stops with `TABLE_OR_VIEW_NOT_FOUND` and Compose restarts it.

## How it runs

| Behaviour | What it means |
| --- | --- |
| One query per topic | Reads `lake.bronze.<entity>` as an Iceberg stream and appends to `lake.silver.<entity>`, partitioned by `days(_changed_at)` |
| Columns | The key, business and audit columns as in bronze; `_commit_sequence`, `_change_op`, `_changed_at` and `_content_hash`; and `_is_deleted`. A delete keeps the row's last values, because the source uses `REPLICA IDENTITY FULL` |
| Duplicate changes dropped | `dropDuplicates` on the change identity (key, `_commit_sequence`, `_change_op`), so a change delivered twice gets one row. No watermark: a late change is appended, never dropped |
| Unchanged versions kept | Re-snapshots and audit-only updates keep their own rows; the views fold them away |
| State in RocksDB | On the executors' disks, with changelog checkpointing; 2 shuffle partitions, one per core. A query keeps its state partitions for life, so changing them needs [a reset](#resetting-silver) |
| Exactly once | Checkpoints record the bronze snapshots read and the deduplication state; a change seen before a restart is still dropped after it |
| Trigger | Every `SILVER_TRIGGER_SECONDS`; `--available-now` processes what bronze has committed and stops. An available-now restart fails once maintenance has expired its first snapshot |
| Described from the Avro schemas | As in bronze, from `tables/silver_template.avsc` |

## Views

Two Iceberg views per entity sit next to the table and fold away the versions that change nothing.

| View | Holds |
| --- | --- |
| `<entity>_history` | One row per version that changed the row, with `_valid_from`, `_valid_to` and `_is_current`. A delete is a version of its own |
| `<entity>_current` | One row per live key: the current, non-deleted versions of the history view |

- **Fold rule:** per key, ordered by `_commit_sequence`, a version folds into the previous one when both `_content_hash` and `_is_deleted` match. A delete does not fold, and neither does a re-create after it.
- **Late versions** take their place by `_commit_sequence`, because the views compute over the whole table when read.
- Gold streams the table, not the views, and applies the same fold rule ([inputs from silver](gold/model.md#inputs-from-silver)).

The streaming job does not manage the views. `make lake-views` creates or updates them, and the silver and gold views in the lake, and replaces a view only when its SQL or descriptions changed. `make pipeline` runs it once as the `lake-views` service, which retries until the silver tables exist. Lake maintenance also refreshes them.

```sql
-- Every version of customer 7, oldest first
SELECT customer_id, customer_name, _change_op, _valid_from, _valid_to, _is_current
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

## Resetting silver

Needed when the state layout changes (for example the number of state partitions). Bronze is not touched.

1. `make pipeline-stop PIPELINE=silver`.
2. In `make spark-sql`, `DROP TABLE lake.silver.<entity> PURGE;` for each of the four entities.
3. Delete `<CHECKPOINT_ROOT>/silver/`; locally `curl -X DELETE "http://localhost:8888/buckets/lake/checkpoints/silver/?recursive=true"`.
4. `make pipeline PIPELINE=silver` recreates the tables and versions every bronze row again.
