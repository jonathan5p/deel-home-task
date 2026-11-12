# Gold

Gold is the serving layer: a star schema in PostgreSQL that answers the four business questions with near-real-time current state and keeps the history of every change. Spark builds it from the silver history tables in the lake.

**Gold is stored in PostgreSQL and computed in the lake.** Bronze and silver are Iceberg tables in the lake (SeaweedFS); gold is the `analytics` database on the `analytics-db` PostgreSQL service, in two schemas: `gold_internal` (the tables the pipeline writes) and `gold` (the views consumers read). The lake has the same model as Iceberg views over silver, `lake.gold.<relation>`, which store nothing.

Gold is built from the silver history tables: SCD2 customer and product dimensions, seeded date and order-status dimensions, append-only history facts for orders and order items (partitioned by month, 3 years kept), and current-state tables for orders and items that the four business questions read.

In short:

- **Open order:** current status other than `COMPLETED`, not deleted; `gold.open_orders` lists them. **Pending orders** are open orders, and **pending items** are the items of open orders.
- **Current state only moves forward:** a version replaces the current row only if its `_commit_sequence` is higher, so late or replayed events never overwrite newer state; they are still kept as history.
- **Top 3 questions include ties**, so more than three rows can come back.

## PostgreSQL gold and the lake gold views

| | PostgreSQL gold | Lake gold views |
| --- | --- | --- |
| Where | Database `analytics`: tables in `gold_internal`, views in `gold` | Iceberg views `lake.gold.<relation>` |
| Written by | The gold job, every micro-batch | Nothing: `make lake-views` defines them, and they compute gold from silver at read time |
| History | History facts keep 36 months by default (`GOLD_RETENTION_MONTHS`) | The full history of silver |
| Keys | Surrogate keys (`customer_key`, `product_key`) | Natural keys (`customer_id`, `product_id`) |
| Read by | The [analytics API](../../serving/api.md), [Grafana](../../serving/grafana.md), `psql` | Spark SQL ([querying the lake](../../serving/lake-access.md)) |

`make data-quality CHECK=gold` compares the two, view by view ([data quality checks](../data-quality.md)).

- [Gold model](model.md): the inputs from silver, the internal tables and public views, the star schema and the grain of every table, the versioning and open-order rules, the business-question views, their indexes and the things to know about the source data.
- [Gold writer](writer.md): the gold DDL, the gold job and how it writes each micro-batch, current state and retention, rebuilding gold from silver, and gold in the lake.
- [Table reference](../../reference/gold/index.md): every gold table and view, with its columns, types and descriptions.
