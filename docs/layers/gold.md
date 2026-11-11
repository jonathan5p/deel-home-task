# Gold

Gold is the dimensional model in PostgreSQL that the business questions read, plus the same model as Iceberg views in the lake. Its design, rules and indexes are in [the gold model](../architecture/gold-model.md); every table and column is in the [table reference](../reference/gold/index.md).

## Gold in PostgreSQL

Gold is a star schema built from the silver history tables: SCD2 customer and product dimensions, seeded date and order-status dimensions, append-only history facts for orders and order items (partitioned by month, 3 years kept), and current-state tables for orders and items that the four business questions read. It uses two schemas:

- `gold_internal` holds the tables the pipeline writes, with the metadata it needs to skip replayed batches and order versions (`_commit_sequence`, soft deletes in `_is_deleted`, `_content_hash` to fold unchanged versions), plus the unlogged staging tables `stage_dim_customer`, `stage_dim_product`, `stage_fact_order_history` and `stage_fact_order_item_history` that the gold job loads every micro-batch into, and the views `dim_customer_versions` and `dim_product_versions`, which derive each dimension version's fold flag, validity range and current flag.
- `gold` holds the views consumers read, with the same names, without that metadata: the history views leave out unchanged changes and add each change's validity range, the current-state views leave deleted rows out, and the dimension views leave out folded and deleted versions. It also holds `open_orders` (the current orders that are open), one view per business question, and `data_freshness`.

The design, with the ERD, the grain of every table, the open and pending rules, versioning by `_commit_sequence`, and retention, is in [docs/architecture/gold-model.md](../architecture/gold-model.md).

In short:

- **Open order:** current status other than `COMPLETED`, not deleted; `gold.open_orders` lists them. **Pending orders** are open orders, and **pending items** are the items of open orders.
- **Current state only moves forward:** a version replaces the current row only if its `_commit_sequence` is higher, so late or replayed events never overwrite newer state; they are still kept as history.
- **Top 3 questions include ties**, so more than three rows can come back.

`docker/analytics-db/initdb/02-gold-model.sql` is generated and git-ignored: `make up` (and `make pipeline` and `make test-infra`) renders it, on the host with [uv](https://docs.astral.sh/uv/), from the Jinja template `analytics/pipelines/src/pipelines/gold/ddl.sql.j2` and the gold Avro schemas (see [table and column definitions](../development/table-definitions.md)), which give every column its type, nullability and description. Edit the template or the schemas, never the SQL file. The file creates both schemas, the tables, views and seeds, and each history table's default partition; the monthly history partitions are created and dropped by the retention job (see [Gold retention](../operations/maintenance-and-recovery.md#gold-retention)). It runs on an empty volume like the other init scripts, and every statement is idempotent, so it can also be applied to an existing database without losing data:

```sh
docker compose exec -T analytics-db psql -v ON_ERROR_STOP=1 -U analytics -d analytics < docker/analytics-db/initdb/02-gold-model.sql
```

Applying it again only adds what is missing: `CREATE TABLE IF NOT EXISTS` leaves an existing table as it is, so a new column or constraint on a gold table does not reach a database created before it. To rebuild only gold, leaving the lake and the Iceberg catalog alone:

```sh
make gold-reset CONFIRM=1   # Stops the gold job, recreates gold and gold_internal from the rendered DDL, deletes the gold checkpoints
make pipeline PIPELINE=gold # The gold job reloads everything from silver
```

The same reset followed by one batch load from the silver tables, optionally only up to a point in time:

```sh
make gold-rebuild CONFIRM=1                              # gold-reset, then reload every silver change and apply retention
make gold-rebuild CONFIRM=1 TO_TIME=2026-11-09T12:00:00  # only the changes committed at or before that time (UTC unless it has an offset)
make pipeline PIPELINE=gold                              # resume streaming afterwards
```

See [Rebuilding gold from silver](../operations/maintenance-and-recovery.md#rebuilding-gold-from-silver) for how it works.

**Data loss:** all gold data is deleted; gold is rebuilt from silver, which keeps the full history. The gold checkpoints (`<CHECKPOINT_ROOT>/gold/`, deleted through the SeaweedFS filer; set `GOLD_CHECKPOINTS` to its path for another `CHECKPOINT_ROOT`) must go with the data, or the job would resume after the batches the dropped rows came from.

The four business questions are views in `gold`, each with a ready-made query in `analytics/sql/gold/` that reads it with the order the question is reported in:

| View | Query | Question |
| --- | --- | --- |
| `gold.open_orders_by_delivery_date_and_status` | `01_open_orders_by_delivery_date_and_status.sql` | Number of open orders by delivery date and status |
| `gold.top_delivery_dates_by_open_orders` | `02_top_delivery_dates_by_open_orders.sql` | Top 3 delivery dates with more open orders |
| `gold.pending_items_by_product` | `03_pending_items_by_product.sql` | Number of open pending items by product (items and total quantity) |
| `gold.top_customers_by_pending_orders` | `04_top_customers_by_pending_orders.sql` | Top 3 customers with more pending orders |

A view has no row order, so add an `ORDER BY` when you query one directly. The views, their columns and the indexes behind them are described in [the gold model](../architecture/gold-model.md#the-four-business-questions).

```sh
docker compose exec -T analytics-db psql -U analytics -d analytics < analytics/sql/gold/01_open_orders_by_delivery_date_and_status.sql
docker compose exec analytics-db psql -U analytics -d analytics -c "select * from gold.top_customers_by_pending_orders order by pending_orders desc, customer_id"
```

`gold.data_freshness` shows, per entity (customers, products, orders, order items), how up to date gold is. For now it is computed from the data, as the source commit time of the newest change gold holds, so it ages when the source stops changing even though the pipeline is healthy. Later the pipeline metrics (#127) will fill it; its shape stays the same.

```sh
docker compose exec analytics-db psql -U analytics -d analytics -c "select * from gold.data_freshness"
```

## Gold job

The gold job (`analytics/pipelines/src/pipelines/gold/job.py`) keeps the gold model in the `analytics` database up to date from the silver tables `<ICEBERG_CATALOG>.silver.<entity>`: the SCD2 dimensions `gold_internal.dim_customer` and `gold_internal.dim_product` from `customers` and `products`, and the history facts `gold_internal.fact_order_history` and `gold_internal.fact_order_item_history` with their current-state tables `current_orders` and `current_order_items` from `orders` and `order_items`. It needs the four silver tables to exist when it starts, and the gold DDL to be applied (`make up`):

```sh
make pipeline PIPELINE=gold   # as a long-running service, UI on http://localhost:4045
make spark-submit SPARK_APP=analytics/pipelines/src/pipelines/gold/job.py SPARK_APP_ARGS=--available-now SPARK_CONF="spark.cores.max=2" SPARK_UI_PORT=4045   # or once, ad hoc
```

- **Four streaming queries,** one per silver table (customers, products, orders, order items), reading it with Iceberg's streaming source, with its checkpoint in `<CHECKPOINT_ROOT>/gold/<table>/` (`dim_customer`, `dim_product`, `fact_order_history`, `fact_order_item_history`). Micro-batches run every `GOLD_TRIGGER_SECONDS`; `--available-now` processes what silver holds and stops.
- **Every micro-batch** is loaded by Spark's JDBC writer into the stream's unlogged staging table (`gold_internal.stage_<table>`, emptied first), then merged by the driver in one PostgreSQL transaction ([psycopg](https://www.psycopg.org/psycopg3/)). For a dimension, only the versions not stored yet are inserted: the dimensions are append-only, and the fold flag, the validity range and the current flag are computed when they are read, by the `gold_internal.<table>_versions` views, from all of a key's stored versions. For orders and order items, every change is appended to the history fact and the latest change of each key replaces its current-state row only if its `_commit_sequence` is higher. A replayed batch, a duplicate or a late change therefore lands correctly, and a failed batch is simply replayed.
- **Unchanged versions are folded, not dropped:** a version whose `_content_hash` and `_is_deleted` equal the previous version's is kept, so a late version that arrives between two equal ones can turn it back into a real version. The dimension versions views mark it with `_is_folded`, and the `gold` dimension views read them and leave folded and deleted versions out; the `gold` history views of the facts leave unchanged changes out at read time.
- **Deletes are final:** a delete is stored as the key's last version, so the key has no current version afterwards, and replays or late events cannot bring it back; the `gold` current-state views leave deleted orders and items out.
- **Dimension keys as of the change:** a history fact row takes the customer or product version valid at its `_changed_at` (`customer_key`, or `product_key` with its `unit_price` and `line_amount`), or the first version when the change is older than it, as happens when tables are snapshotted a few milliseconds apart; null while there is none or after a delete. A dimension merge re-resolves the fact rows of the keys it touched, so a customer or product that arrives late, or a late version that changes a validity range, still ends in the right key; the lookup reads the dimension versions views, so it applies the same fold and validity rule as the `gold` dimension views. Every merge first takes one PostgreSQL advisory lock, so these merges never run side by side and miss each other's rows.
- **Early items:** an item that arrives before its order is stored; the business views count it once the order arrives.
- **The column lists come from the gold Avro schemas:** the staging tables, the merge SQL and the dimension versions views are built from the gold `.avsc` files; a column is loaded from the silver column its `from` names, a metadata column from the silver column of the same name, and `_valid_from` from `_changed_at` (`unit_price` from `unity_price`); the facts' status, date and dimension keys come from lookups declared in `pipelines/gold/dimensions.py`.
- **Fail fast:** like the bronze job, it stops every query when one stops or fails, and exits non-zero if one failed.

The details, with the fold rule, the transactions and the dimension keys of the facts, are in [Writing gold](../architecture/gold-model.md#writing-gold).

## Gold views in the lake

The gold model can also be queried in the lake, without PostgreSQL: `lake.gold` holds one Iceberg view per public gold relation (`dim_customer`, `dim_product`, `dim_date`, `dim_order_status`, `fact_order_history`, `fact_order_item_history`, `current_orders`, `current_order_items`, `open_orders`, the four business-question views and `data_freshness`). They copy no data and are not written by the gold job: they compute gold at read time from the silver tables and their `_history` views, with the same rules as PostgreSQL gold, so the lake keeps the full history (PostgreSQL's retention does not apply). `make lake-views` creates them right after the silver views, and replaces only those whose query or descriptions changed; every lake maintenance run does the same (see [Lake maintenance](../operations/maintenance-and-recovery.md#lake-maintenance)).

- **Same columns as the PostgreSQL `gold` views, without the surrogate keys:** facts refer to the dimensions by their natural keys (`customer_id`, `product_id`) and carry the attributes resolved as of each change (`unit_price` and `line_amount`); the dimension views have `_valid_from`, `_valid_to` and `_is_current` for point-in-time joins.
- **Built from the same declarations as the gold writer:** `pipelines/gold/lake/views.py` renders the SQL templates next to it (`dimension.sql.j2`, `fact.sql.j2`, ...) from the gold Avro schemas and the lookups in `pipelines/gold/dimensions.py`; the descriptions are the public ones of the gold schemas. The calendar and the statuses are generated in the views (`dim_date.sql.j2`, `dim_order_status.sql.j2`); an infra test checks they equal PostgreSQL's seeds.

```sql
-- Question 2 of the take-home, from the lake: the top 3 delivery dates by open orders, ties included
SELECT delivery_date, open_orders
FROM lake.gold.top_delivery_dates_by_open_orders
ORDER BY open_orders DESC, delivery_date;
```

The rules, the point-in-time join and how they relate to PostgreSQL gold are in [Gold in the lake](../architecture/gold-model.md#gold-in-the-lake).
