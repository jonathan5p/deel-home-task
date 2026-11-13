# Gold writer

How gold is written: the gold job's four streams, the copy into PostgreSQL, retention, rebuilds and the views in the lake. The model is in [the gold model](model.md); every column is in the [table reference](../../reference/gold/index.md).

## The gold DDL

- **Iceberg tables** `lake.gold_internal.<table>` are created by the gold job on start, from the gold Avro schemas ([table definitions](../../development/table-definitions.md)): format v2, merge-on-read, history facts bucketed by business key.
- **PostgreSQL DDL** is generated from the schemas and templates when the stack starts. Edit the templates, not the generated SQL.

There are no migration commands. A change to a stored table means resetting or rebuilding gold; silver and the rest of the lake stay as they are:

```sh
make gold-reset CONFIRM=1                                # drop gold in PostgreSQL and the lake, and its checkpoints
make pipeline PIPELINE=gold                              # the gold job recreates its tables and reloads from silver
make gold-rebuild CONFIRM=1                              # or reset, then rebuild from silver in one batch
make gold-rebuild CONFIRM=1 TO_TIME=2026-11-09T12:00:00  # only the changes committed up to that time
```

A reset deletes all gold data; silver keeps the full history, and the surrogate keys come back with the same values ([rebuilding gold](../../operations/maintenance-and-recovery.md#rebuilding-gold-from-silver)).

## Gold job

```sh
make pipeline PIPELINE=gold   # as a service, UI on http://localhost:4045
```

It needs the four silver tables. On start it creates missing Iceberg tables, merges the seeded dimensions, copies into PostgreSQL any table whose latest snapshot PostgreSQL lacks, and creates the lake gold views.

| Silver table | Stream and checkpoint | Writes |
| --- | --- | --- |
| `customers` | `dim_customer` | `dim_customer` |
| `products` | `dim_product` | `dim_product` |
| `orders` | `fact_order_history` | `fact_order_history`, `current_orders` |
| `order_items` | `fact_order_item_history` | `fact_order_item_history`, `current_order_items` |

- **No lock between streams.** No stream reads another's tables; the views join the dimension version valid at each change when read ([point-in-time attributes](model.md#point-in-time-dimension-attributes)). Each stream runs in its own fair-scheduler pool.
- **No transformation runs in PostgreSQL.** Spark computes every row; PostgreSQL receives plain upserts.
- **Column lists come from the gold Avro schemas;** status and date keys come from broadcast joins to the seeded dimensions.
- **Trigger** every `GOLD_TRIGGER_SECONDS`; `--available-now` processes what silver holds and stops.
- **One `GOLD BATCH` log line per batch,** with each step's time and Spark jobs ([monitoring](../../operations/monitoring.md#gold-batch-log)).
- **Fail fast:** when one stream fails, the job stops the others and exits non-zero.

## Writing gold

Each batch of a stream runs these steps:

1. **Stage** the batch with its status and date keys, once.
2. **Catch up:** if an earlier batch wrote Iceberg but failed before the PostgreSQL copy, copy that table first.
3. **Compute each table once:** read the stored rows of the keys the batch touches, rank them with the batch's changes in one window by key, and keep only new or changed rows. The result is cut from its lineage (`localCheckpoint`), so the merge and the copy read the same rows.
4. **Merge into Iceberg,** one `mergeInto` per table with rows to write.
5. **Copy to PostgreSQL** in one transaction, with the new Iceberg snapshot ids.

| Write mode | Tables | Iceberg | PostgreSQL |
| --- | --- | --- | --- |
| Merge | Dimensions, history facts | Matched on (key, `_commit_sequence`): update; else insert | `INSERT ... ON CONFLICT DO UPDATE` |
| Merge newer | Current state | Matched on key and the incoming `_commit_sequence` higher: update; else insert | `INSERT ... ON CONFLICT DO UPDATE` |

**Idempotency.** Every write depends only on the stored rows and the batch: a replayed change is skipped, an unchanged row is not written, current state only takes a higher `_commit_sequence`, and the copy upserts by key. A batch that fails anywhere is replayed and ends in the same rows. An Iceberg commit conflict, for example with compaction, replays the batch up to three times.

## Copying into PostgreSQL

1. **Stage:** Spark's JDBC writer fills the unlogged staging table `gold_internal.stage_<table>` with the batch's rows. History older than the retention window is left out, so a dropped month never comes back.
2. **Upsert in one transaction:** the driver takes the advisory lock in shared mode, upserts each table, and records each table's Iceberg snapshot id in `gold_internal.sync_state`.

The four streams copy at the same time; [retention](#current-state-and-retention) takes the lock exclusively. Because `sync_state` commits with the rows, PostgreSQL always knows which Iceberg snapshot it holds. Only when a copy is missing, at start-up or after a failed batch, does the job copy the difference between the snapshot PostgreSQL holds and the current one, or the whole table when that snapshot is gone.

## Consistency between the two copies

| Where | Consistent | What a reader can see |
| --- | --- | --- |
| PostgreSQL | Per stream batch: a fact's history and current state commit together | An order change before its customer's new version; the views resolve it once the version arrives |
| Iceberg | Per table | A fact's new history briefly before its current state |
| Between the two | PostgreSQL holds exactly the Iceberg rows at the snapshot `sync_state` records | The same answers from the same SQL; `make data-quality CHECK=gold` checks it |

**Freshness.** `gold.data_freshness` reports per entity the source commit time of the newest change gold holds. It ages when the source stops changing, even when the pipeline is healthy; pipeline statistics in an `observability` catalog would replace it ([D33](../../architecture/decisions.md#d33--pipeline-metrics-in-a-separate-observability-catalog-proposed)).

## Current state and retention

**Current state is stored in tables,** so the business-question views read only current rows however long the history grows. A view over history would slow down as it grows, a materialized view would recompute everything per batch, and both would lose open orders older than the retention window.

**Retention.** Iceberg keeps every change. PostgreSQL's history facts are partitioned by month on `_changed_at` and keep 36 months; current state and dimensions are never trimmed. The retention job (once on `make up` and `make pipeline`, then daily in `gold-retention-job`) calls `gold_internal.apply_history_retention` in one transaction under the exclusive advisory lock:

1. Create the months ahead, and move any rows in the default partition into partitions of their own (late changes, or a reload, land there).
2. Drop every monthly partition that ends before the window; a month partly inside it is kept.

Taking the advisory lock first means retention and the copies always lock in the same order and never deadlock.

## Rebuilding from silver

`make gold-rebuild CONFIRM=1 [TO_TIME=...]` resets gold, then:

1. Recreates the Iceberg tables, merges the seeds and creates the lake gold views.
2. For each stream, reads its silver table up to `TO_TIME` and writes it with the gold job's own writer, in one batch, to Iceberg.
3. Reloads every PostgreSQL table from Iceberg and records the snapshots.
4. Applies retention.

A rebuild up to a cutoff matches what the stream would have built from the changes committed up to then, late arrivals included. The keys keep their values. The gold job is left stopped; restarted on fresh checkpoints, it finds every change up to the cutoff already stored and continues past it.

## Gold in the lake

`lake.gold` holds the same 14 views as PostgreSQL's `gold` schema, rendered from the same templates over `lake.gold_internal`: the dimensions, the point-in-time facts, the current state, `open_orders`, the four business-question views and `data_freshness`. They show the same columns and keys, and give the same answers, except for history PostgreSQL's retention has dropped.

- The gold job creates or replaces the views on start, after its tables; `make lake-views` and lake maintenance refresh them, and skip them while the tables do not exist.
- The views are stored as Spark SQL. An engine that cannot run them reads the `lake.gold_internal` tables with the views' SQL ([querying the lake](../../serving/lake-access.md)).

A point-in-time fact carries its dimension version's key, so it joins the rest of that version directly:

```sql
SELECT i.order_item_id, i._changed_at, i.product_name, i.unit_price, p.barcode
FROM lake.gold.fact_order_item_history AS i
JOIN lake.gold.dim_product AS p ON p.product_key = i.product_key;
```
