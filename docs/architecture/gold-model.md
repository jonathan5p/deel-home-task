# Gold dimensional model (PostgreSQL)

**Gold lives in PostgreSQL, not in the lake.** Bronze and silver are Iceberg tables in the lake (SeaweedFS); gold is the `analytics` database on the `analytics-db` PostgreSQL service, in two schemas: `gold_internal` (the tables the pipeline writes) and `gold` (the views consumers read).

Gold is the serving layer: a star schema in PostgreSQL that answers the four business questions with near-real-time current state and keeps the history of every change. Spark builds it from the silver history tables in the lake, and writes the same rows to Iceberg gold tables in the lake as a second sink.

## Inputs from silver

Silver keeps one append-only table per source entity, `lake.silver.<entity>`, with one row per source change: the entity's source and audit columns under their lake names (so `unity_price`, and `quantity` for the misspelled `quanity`), plus:

| Column | Meaning |
| --- | --- |
| `_commit_sequence` | the change's position in the source database's write-ahead log (Debezium `source.lsn`); increases in commit order |
| `_change_op` | `snapshot`, `create`, `update` or `delete` |
| `_changed_at` | when the change was committed in the source |
| `_content_hash` | hex SHA-256 of the content columns (key and audit columns excluded) |
| `_is_deleted` | whether the change is a delete; the row keeps its last known values |

Exact duplicates are already dropped. Gold reads these tables, not silver's views, because a streaming query can only read a table.

**Unchanged versions.** Silver keeps re-snapshots and audit-only updates as rows of their own. Per key, ordered by `_commit_sequence`, a version whose `_content_hash` and `_is_deleted` both equal the previous version's is not a new version: the silver `_history` view folds it, and gold's writer must skip it.

Gold applies the rule without dropping rows: bronze replays can append a missing change late, and a late version that lands between two equal versions turns the later one back into a real change. So the dimensions keep every version silver sends, are only ever appended to, and the views `gold_internal.dim_customer_versions` and `gold_internal.dim_product_versions` derive the folded ones when they are read:

- `_is_folded` is true when a version's `_content_hash` and `_is_deleted` are not distinct from the previous stored version's by `_commit_sequence`; the first version of a key is never folded. Because it is computed from all stored versions on every read, a late version changes it without any update.
- Folded versions are never current and the `gold` views leave them out. Their `_valid_to` is the one of the version they repeat, so a folded version ends when that version ends.
- Example: product 3 at price 3.00 is stored at positions 60 and 80 (a re-snapshot): 80 is folded, 60 is current. A late update to 4.00 at position 70 arrives: 60 is valid until 70, 70 until 80, and 80 is no longer folded and becomes current.

### What `_commit_sequence` is

`_commit_sequence` is the PostgreSQL log sequence number (LSN) of the change, as a 64-bit integer: the byte position of the change in the source database's write-ahead log. It only grows, in commit order, across the whole database. For one key, a later change always has a higher position than an earlier one; snapshot rows carry the position of the snapshot, which is lower than any change streamed after it. Gold uses it to order versions of a key and to tell a late or replayed event from a newer one. Arrival order and Kafka offsets are not used for ordering.

## Internal tables and public views

Gold has two schemas:

- **`gold_internal`** holds the tables. Only the gold streaming query writes them, and they keep the metadata it needs: `_commit_sequence` to skip replayed batches and to order versions, `_is_deleted` for soft deletes, and in the dimensions `_content_hash` to fold unchanged versions. It also holds the staging tables the gold job loads every micro-batch into (see [Writing gold](#writing-gold)), and the dimension versions views, the one place that derives `_is_folded`, `_valid_to` and `_is_current`: the public dimension views and the facts' as-of lookups both read them.
- **`gold`** holds one view per table, with the same name, for consumers: the business queries, the API and anyone browsing the model. The views leave out `_commit_sequence`, `_content_hash`, `_is_deleted` and `_is_folded`; the current-state views leave out deleted rows, the dimension views leave out folded and deleted versions, and the history views add each version's validity range.

Consumers never need to know how the pipeline orders or deduplicates changes, and the tables can change for the pipeline without breaking them.

| Column | In `gold_internal` | In the `gold` views |
| --- | --- | --- |
| `_commit_sequence` | every table except `dim_date` and `dim_order_status` | left out |
| `_is_deleted` | `dim_customer`, `dim_product`, `current_orders`, `current_order_items` | left out; deleted rows are filtered |
| `_content_hash` | `dim_customer`, `dim_product` | left out |
| `_is_folded` | not stored; computed by `dim_customer_versions`, `dim_product_versions` | left out; folded versions are filtered |
| `_valid_to`, `_is_current` of the dimensions | not stored; computed by `dim_customer_versions`, `dim_product_versions` | taken from the versions views |
| `_valid_to`, `_is_current` of history facts | not stored | computed from the next version by `_commit_sequence` |

## Where the definitions live

Every gold table's columns, their types and nullability, and every table, view and column description are defined once, in the gold Avro schemas in `analytics/pipelines/src/pipelines/schemas/gold/`, one `<table>.avsc` per table (plus `data_freshness.avsc` for that view). A column that comes from a source table takes its description from there with `"from"` (for example `"from": "orders.delivery_date"`), so the lake and gold describe it the same way; a metadata column such as `_commit_sequence` is defined in full, exactly like in the bronze template, and a unit test keeps every definition of a `_` column the same across all schemas; a column only gold has, such as a surrogate key or `line_amount`, has its own `doc`. The record's `doc` describes the table in `gold_internal` and `public_doc` its view in `gold` when that differs.

The DDL itself is the Jinja template `analytics/pipelines/src/pipelines/gold/ddl.sql.j2`: it keeps the keys, constraints, partitions, indexes, seeds and view queries, and takes the column lists and the descriptions from the schemas. The public views are described like their tables, without the pipeline-only columns, plus the columns they add (`_valid_to` and `_is_current`, described like the columns of the same name in the dimension versions views, whose schemas are `dim_customer_versions.avsc` and `dim_product_versions.avsc`). `make up` renders it to the git-ignored `docker/analytics-db/initdb/02-gold-model.sql`, which PostgreSQL runs on an empty volume.

## Model

The diagram shows the `gold` views in the PostgreSQL `analytics` database; the keys and relations are enforced on the `gold_internal` tables.

```mermaid
---
title: Gold star schema in PostgreSQL (analytics database, schema gold)
---
erDiagram
    dim_customer ||--o{ fact_order_history : "customer_key"
    dim_order_status ||--o{ fact_order_history : "status_key"
    dim_date ||--o{ fact_order_history : "order_date_key, delivery_date_key"
    dim_product ||--o{ fact_order_item_history : "product_key"
    dim_order_status ||--o{ current_orders : "status_key"
    dim_customer ||--o{ current_orders : "customer_id (current version)"
    dim_date ||--o{ current_orders : "order_date_key, delivery_date_key"
    current_orders ||--o{ current_order_items : "order_id"
    dim_product ||--o{ current_order_items : "product_id (current version)"

    dim_customer {
        bigint customer_key PK
        bigint customer_id
        varchar customer_name
        varchar customer_address
        boolean is_active
        timestamp _valid_from
        timestamp _valid_to
        boolean _is_current
    }
    dim_product {
        bigint product_key PK
        bigint product_id
        varchar product_name
        varchar barcode
        numeric unit_price
        boolean is_active
        timestamp _valid_from
        timestamp _valid_to
        boolean _is_current
    }
    dim_date {
        int date_key PK
        date calendar_date
        int year
        int quarter
        int month
        int day_of_month
        int day_of_week
        boolean is_weekend
    }
    dim_order_status {
        int status_key PK
        varchar status_code
        boolean is_open
    }
    fact_order_history {
        bigint order_id
        timestamp _changed_at
        varchar _change_op
        int status_key FK
        bigint customer_id
        bigint customer_key FK
        int order_date_key FK
        int delivery_date_key FK
        timestamp _valid_to
        boolean _is_current
    }
    fact_order_item_history {
        bigint order_item_id
        timestamp _changed_at
        varchar _change_op
        bigint order_id
        bigint product_id
        bigint product_key FK
        int quantity
        numeric unit_price
        numeric line_amount
        timestamp _valid_to
        boolean _is_current
    }
    current_orders {
        bigint order_id PK
        int status_key FK
        bigint customer_id
        date order_date
        date delivery_date
        int order_date_key FK
        int delivery_date_key FK
        timestamp _changed_at
    }
    current_order_items {
        bigint order_item_id PK
        bigint order_id
        bigint product_id
        int quantity
        timestamp _changed_at
    }
```

### Tables and grain

| Table | Kind | Grain | Key (in `gold_internal`) |
| --- | --- | --- | --- |
| `dim_customer` | SCD2 dimension | one row per customer change from silver, folded versions and the delete included, append-only; the view shows one row per customer version | `customer_key` (surrogate); unique (`customer_id`, `_commit_sequence`) |
| `dim_product` | SCD2 dimension | as `dim_customer`, per product; a new price is a new version | `product_key` (surrogate); unique (`product_id`, `_commit_sequence`) |
| `dim_customer_versions`, `dim_product_versions` | internal views | every stored version with `_is_folded`, `_valid_to` and `_is_current`; no public view of their own | as their dimension |
| `stage_dim_customer`, `stage_dim_product` | staging, unlogged | the versions of the micro-batch being written; no view | none |
| `dim_date` | seeded dimension | one row per calendar day, 2000-01-01 to 2099-12-31 | `date_key` (`yyyymmdd`) |
| `dim_order_status` | seeded dimension | one row per status: `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`, `UNKNOWN` | `status_key` |
| `fact_order_history` | history fact | one row per change to an order | (`order_id`, `_commit_sequence`, `_changed_at`) |
| `fact_order_item_history` | history fact | one row per change to an order item | (`order_item_id`, `_commit_sequence`, `_changed_at`) |
| `current_orders` | current state | one row per order; the view leaves deleted orders out | `order_id` |
| `current_order_items` | current state | one row per order item; the view leaves deleted items out | `order_item_id` |

Notes:

- `unity_price` is renamed `unit_price` in gold. `order_id`, `customer_id`, `product_id` and `order_item_id` stay as degenerate keys next to the surrogate keys, so facts can be joined to the source without the dimensions.
- `customer_key` in `fact_order_history` is the customer version valid at the order change's `_changed_at`.
- `product_key`, `unit_price` and `line_amount` (`quantity × unit_price`) in `fact_order_item_history` come from the product version valid at the item's `_changed_at`, which is when the order was placed: the source inserts an order's items right after the order. This keeps the price the order was placed at, even after the product's price changes.
- A delete is a history row with `_change_op = 'delete'` and the row's last values. History has no `_is_deleted` column: `_change_op` already says it.
- The history facts' keys include `_changed_at` because PostgreSQL requires the partition column in the primary key. A change always has the same `_changed_at`, so the key still identifies one change.
- The `gold` history views add `_valid_to` (the next version's `_changed_at`, ordered by `_commit_sequence`) and `_is_current`, so history can be queried as of any time, for example "open orders on 2026-11-01".

## Rules

### Versioning

- **History is idempotent.** A history row is inserted only if (key, `_commit_sequence`) is not already there. Replaying a micro-batch, or a Debezium re-send after a restart, adds nothing.
- **Current state only moves forward.** A version replaces the current row only if its `_commit_sequence` is higher than the stored one. A late version is still appended to history, where its validity range lands in the right place, but it never overwrites newer current state.

  Example: order 7 is stored as `PROCESSING` at position 500. A delayed event `PENDING` at position 420 arrives. History gets the 420 row (valid until the 500 change); `current_orders` stays `PROCESSING`. Then `COMPLETED` at 610 arrives and replaces it.
- **Deletes are soft in current state.** A delete sets `_is_deleted` and keeps the row with the delete's `_commit_sequence`, so a late event older than the delete cannot bring the row back. The `gold` current-state views leave deleted rows out.
- **SCD2 dimensions.** `_valid_from` is the version's `_changed_at`. Among the versions that are not folded, ordered by `_commit_sequence`, each one is valid until the next one's `_valid_from`, and the last one is current unless it is a delete. A new version therefore closes the current row and becomes current itself, and a late older version gets the range between its neighbours. A delete is stored as the key's last version, so the key has no current version afterwards; replays and late events cannot revive it, because they sort before the delete. The versions views compute this on every read from the stored versions, so a key has at most one current version by construction, with no check constraint or partial index to maintain; the unique (key, `_commit_sequence`) index serves their windows, which are all partitioned by the key.

### Open orders and pending items

- **Open order:** an order in `current_orders` that is not deleted and whose status has `is_open`. Every status except `COMPLETED` is open. A null or unexpected status maps to `UNKNOWN`, which is open, so it shows up in the metrics instead of disappearing.

  | Status | Open |
  | --- | --- |
  | `PENDING` | yes |
  | `PROCESSING` | yes |
  | `REPROCESSING` | yes |
  | `UNKNOWN` (null or unexpected) | yes |
  | `COMPLETED` | no |

- **Pending order** (question 4): an open order. `PENDING` alone would leave out orders being processed, and would not match questions 1 to 3.
- **Pending item** (question 3): an item, not deleted, of an open order. Question 3 reports both the number of pending items and their total quantity per product.
- **Top 3:** questions 2 and 4 rank by count with `RANK()` and keep places 1 to 3, so every row tied for third place is kept and more than three rows can come back. `RANK()` leaves gaps after a tie (two dates tied for first are both 1, the next is 3), so ties never add places: `DENSE_RANK()` would count a tie as one place and could return many more rows. Window functions work in any SQL engine, unlike `FETCH FIRST ... WITH TIES`.

Example: customer 3 has order 10 `PENDING` with two items of product 5 (quantities 4 and 6), and order 11 `COMPLETED` with one item of product 5. Customer 3 has one pending order; product 5 has two pending items with a total quantity of 10.

## Current state and retention

**Current state is stored in tables** updated by the streaming queries, in the same database transaction as the history insert. The metric views read only these small tables, so they stay fast as history grows.

The alternatives were rejected:

- A view that picks the latest version from history gets slower as history grows.
- A materialized view recomputes everything on every refresh, too slow for one refresh per micro-batch.
- Both would lose orders that are still open but whose last change is older than the retention window.

**Retention:** the history facts keep 3 years by default in PostgreSQL. They are range-partitioned by month on `_changed_at`, so retention drops whole partitions instead of deleting rows; a default partition catches rows outside the created range. Current-state tables and the dimensions are never trimmed, so an order that is still open keeps its current row however old it is. The lake gold tables keep the full history.

## Writing gold

The gold job (`pipelines/gold/job.py`) runs one streaming query per SCD2 dimension, reading its silver table, and writes every micro-batch in two steps:

1. **Stage.** Spark's JDBC writer replaces the rows of the dimension's staging table, `gold_internal.stage_dim_customer` or `gold_internal.stage_dim_product` (overwrite with `truncate`, so the table and its descriptions stay). The staging tables are unlogged: they hold only the batch being written, so they need no crash safety and write faster.
2. **Merge, in one transaction.** The driver connects with psycopg and, in one PostgreSQL transaction, inserts the staged versions not stored yet (`ON CONFLICT (key, _commit_sequence) DO NOTHING`). Nothing stored is ever updated: the versions views derive `_is_folded`, `_valid_to` and `_is_current` from all of a key's versions ordered by `_commit_sequence` whenever they are read.

```mermaid
sequenceDiagram
    participant S as silver table
    participant J as gold job (driver)
    participant X as executors
    participant P as PostgreSQL
    S->>J: micro-batch of new silver rows
    J->>X: write the batch
    X->>P: TRUNCATE + INSERT stage_dim_* (JDBC)
    J->>P: BEGIN
    J->>P: INSERT new versions ON CONFLICT DO NOTHING
    J->>P: COMMIT
    J->>S: commit the batch in the checkpoint
```

**Idempotency.** Both steps can run again with the same input and leave the same result: staging is replaced, the insert skips stored versions, and the versions views derive the flags from what is stored, not from the batch. A batch replayed after a failure, a whole silver table reprocessed from a fresh checkpoint, a duplicate version and a late older version all end in the same dimension rows. No transaction spans Spark's checkpoint and PostgreSQL, so a batch that fails between them is simply replayed.

**Generic writer.** The staging table's columns, the INSERT column list and the versions view are built from the dimension's gold schema (`pipelines/gold/scd2.py`, with the SQL in the templates `scd2_insert.sql.j2` and `scd2_versions.sql.j2` next to it), never written by hand: a column is loaded from the silver column its `from` names (`<entity>.<lake name>`), a metadata column from the silver column of the same name, `_valid_from` from `_changed_at`, and any other column from a rename the dimension declares (`unit_price` from `unity_price`). The surrogate key is left to PostgreSQL, and the derived columns to the versions view. The history facts, the current-state tables and the Iceberg gold tables in the lake are written the same way in later steps.

**Freshness.** `gold.data_freshness` reports, per entity, how up to date gold is. For now it is computed from the data, as the source commit time of the newest change gold holds, so it ages when the source stops changing even though the pipeline is healthy. Later it will be filled from the pipeline statistics, with the same shape. Those statistics (per micro-batch rows, durations, last `_commit_sequence`, last event time) are planned in a separate `observability` catalog in the lake, apart from the business data (#127).

## The four business questions

| Question | Answered from | Query |
| --- | --- | --- |
| Number of open orders by delivery date and status | `current_orders` joined to `dim_order_status`, where open, grouped by `delivery_date` and `status_code` | [01](../../analytics/sql/gold/01_open_orders_by_delivery_date_and_status.sql) |
| Top 3 delivery dates with more open orders | the same count by `delivery_date`, ordered descending, top 3 with ties | [02](../../analytics/sql/gold/02_top_delivery_dates_by_open_orders.sql) |
| Number of open pending items by product | `current_order_items` joined to open `current_orders`, grouped by `product_id`: item count and total quantity | [03](../../analytics/sql/gold/03_pending_items_by_product.sql) |
| Top 3 customers with more pending orders | open `current_orders` by `customer_id`, with the name from the current `dim_customer`, top 3 with ties | [04](../../analytics/sql/gold/04_top_customers_by_pending_orders.sql) |

The queries are in [`analytics/sql/gold/`](../../analytics/sql/gold/) and read only the `gold` views. For example, the core of question 2:

```sql
SELECT delivery_date, open_orders
FROM (
    SELECT
        o.delivery_date,
        count(*) AS open_orders,
        rank() OVER (ORDER BY count(*) DESC) AS place
    FROM gold.current_orders AS o
    JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
    WHERE s.is_open
    GROUP BY o.delivery_date
) AS ranked_dates
WHERE place <= 3;
```

## Known source issues

- The data generator looks up "today's order of a customer" by customer and date only when inserting items, so new items can be attached to an order of the same day that is already `COMPLETED`. Gold keeps these items; they are not pending because their order is not open.
- `products.unity_price` is a `DECIMAL` without precision or scale, so `unit_price` is an unconstrained `NUMERIC` in gold, which keeps every price exact.
