# Gold dimensional model

Gold is the serving layer: a star schema in PostgreSQL (schema `gold` of the `analytics` database) that answers the four business questions with near-real-time current state and keeps the history of every change. Spark builds it from the silver history tables in the lake, and writes the same rows to Iceberg gold tables in the lake as a second sink.

## Inputs from silver

Silver keeps one row per version of each source entity (customers, products, orders, order items), appended in the order changes reach it. Its views expose, per version:

| Column | Meaning |
| --- | --- |
| entity key | `customer_id`, `product_id`, `order_id` or `order_item_id` |
| business columns | the source columns, with `unity_price` decoded to a decimal and `quanity` renamed to `quantity` |
| `change_position` | the change's position in the source database's write-ahead log (Debezium `source.lsn`) |
| `change_op` | `snapshot`, `create`, `update` or `delete` |
| `changed_at` | when the change was committed in the source |
| `valid_from`, `valid_to`, `is_current` | the version's validity range, derived from the next version by `change_position` |
| `is_deleted`, `content_hash` | whether the version is a delete, and the hash of its business columns |

### What `change_position` is

`change_position` is the PostgreSQL log sequence number (LSN) of the change, as a 64-bit integer: the byte position of the change in the source database's write-ahead log. It only grows, in commit order, across the whole database. For one key, a later change always has a higher position than an earlier one; snapshot rows carry the position of the snapshot, which is lower than any change streamed after it. Gold uses it to order versions of a key and to tell a late or replayed event from a newer one. Arrival order and Kafka offsets are not used for ordering.

## Model

```mermaid
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
        bigint change_position
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }
    dim_product {
        bigint product_key PK
        bigint product_id
        varchar product_name
        varchar barcode
        numeric unit_price
        boolean is_active
        bigint change_position
        timestamp valid_from
        timestamp valid_to
        boolean is_current
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
        smallint status_key PK
        varchar status_code
        boolean is_open
    }
    fact_order_history {
        bigint order_id PK
        bigint change_position PK
        timestamp changed_at PK
        varchar change_op
        smallint status_key FK
        bigint customer_id
        bigint customer_key FK
        int order_date_key FK
        int delivery_date_key FK
    }
    fact_order_item_history {
        bigint order_item_id PK
        bigint change_position PK
        timestamp changed_at PK
        varchar change_op
        bigint order_id
        bigint product_id
        bigint product_key FK
        int quantity
        numeric unit_price
        numeric line_amount
    }
    current_orders {
        bigint order_id PK
        smallint status_key FK
        bigint customer_id
        date order_date
        date delivery_date
        int order_date_key FK
        int delivery_date_key FK
        bigint change_position
        timestamp changed_at
        boolean is_deleted
    }
    current_order_items {
        bigint order_item_id PK
        bigint order_id
        bigint product_id
        int quantity
        bigint change_position
        timestamp changed_at
        boolean is_deleted
    }
```

### Tables and grain

| Table | Kind | Grain | Key |
| --- | --- | --- | --- |
| `dim_customer` | SCD2 dimension | one row per customer version | `customer_key` (surrogate); unique (`customer_id`, `change_position`); one row with `is_current` per `customer_id` |
| `dim_product` | SCD2 dimension | one row per product version; a new price is a new version | `product_key` (surrogate); unique (`product_id`, `change_position`); one row with `is_current` per `product_id` |
| `dim_date` | seeded dimension | one row per calendar day, 2000-01-01 to 2099-12-31 | `date_key` (`yyyymmdd`) |
| `dim_order_status` | seeded dimension | one row per status: `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`, `UNKNOWN` | `status_key` |
| `fact_order_history` | history fact | one row per change to an order | (`order_id`, `change_position`, `changed_at`) |
| `fact_order_item_history` | history fact | one row per change to an order item | (`order_item_id`, `change_position`, `changed_at`) |
| `current_orders` | current state | one row per order | `order_id` |
| `current_order_items` | current state | one row per order item | `order_item_id` |

Notes:

- `unity_price` is renamed `unit_price` in gold. `order_id`, `customer_id`, `product_id` and `order_item_id` stay as degenerate keys next to the surrogate keys, so facts can be joined to the source without the dimensions.
- `customer_key` in `fact_order_history` is the customer version valid at the order change's `changed_at`.
- `product_key`, `unit_price` and `line_amount` (`quantity × unit_price`) in `fact_order_item_history` come from the product version valid at the item's `changed_at`, which is when the order was placed: the source inserts an order's items right after the order. This keeps the price the order was placed at, even after the product's price changes.
- A delete is a history row with `change_op = 'delete'` and the row's last values. History has no `is_deleted` column: `change_op` already says it.
- The history facts' keys include `changed_at` because PostgreSQL requires the partition column in the primary key. A change always has the same `changed_at`, so the key still identifies one change.
- Views on the history facts add `valid_to` (the next version's `changed_at`, ordered by `change_position`) and `is_current`, so history can be queried as of any time, for example "open orders on 2026-11-01".

## Rules

### Versioning

- **History is idempotent.** A history row is inserted only if (key, `change_position`) is not already there. Replaying a micro-batch, or a Debezium re-send after a restart, adds nothing.
- **Current state only moves forward.** A version replaces the current row only if its `change_position` is higher than the stored one. A late version is still appended to history, where its validity range lands in the right place, but it never overwrites newer current state.

  Example: order 7 is stored as `PROCESSING` at position 500. A delayed event `PENDING` at position 420 arrives. History gets the 420 row (valid until the 500 change); `current_orders` stays `PROCESSING`. Then `COMPLETED` at 610 arrives and replaces it.
- **Deletes are soft in current state.** A delete sets `is_deleted` and keeps the row with the delete's `change_position`, so a late event older than the delete cannot bring the row back.
- **SCD2 dimensions.** A new version closes the current row (`valid_to` = the new version's `valid_from`, `is_current` false) and inserts the new one. Silver already skips versions whose business columns did not change.

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
- **Top 3:** questions 2 and 4 include ties (`FETCH FIRST 3 ROWS WITH TIES`), so more than three rows come back when several share the third place.

Example: customer 3 has order 10 `PENDING` with two items of product 5 (quantities 4 and 6), and order 11 `COMPLETED` with one item of product 5. Customer 3 has one pending order; product 5 has two pending items with a total quantity of 10.

## Current state and retention

**Current state is stored in tables** updated by the streaming queries, in the same database transaction as the history insert. The metric views read only these small tables, so they stay fast as history grows.

The alternatives were rejected:

- A view that picks the latest version from history gets slower as history grows.
- A materialized view recomputes everything on every refresh, too slow for one refresh per micro-batch.
- Both would lose orders that are still open but whose last change is older than the retention window.

**Retention:** the history facts keep 3 years by default in PostgreSQL. They are range-partitioned by month on `changed_at`, so retention drops whole partitions instead of deleting rows; a default partition catches rows outside the created range. Current-state tables and the dimensions are never trimmed, so an order that is still open keeps its current row however old it is. The lake gold tables keep the full history.

## Writing gold

For each micro-batch, the gold streaming query:

1. writes the batch's versions to staging tables in PostgreSQL;
2. in one transaction: closes and inserts SCD2 dimension versions, inserts the new history rows, applies newer versions to the current-state tables;
3. merges the same rows into the Iceberg gold tables in the lake, by key and `change_position`; they are partitioned by month and keep the full history.

No transaction spans PostgreSQL and the lake, so both writes are idempotent: a failed batch is simply replayed.

Pipeline statistics (per micro-batch: rows, durations, last `change_position`, last event time, and so freshness) are not stored in gold. They go to `observability.pipelines.batch_metrics`, an Iceberg table in a separate `observability` catalog in the lake, apart from the business data.

## The four business questions

| Question | Answered from |
| --- | --- |
| Number of open orders by delivery date and status | `current_orders` joined to `dim_order_status`, where open, grouped by `delivery_date` and `status_code` |
| Top 3 delivery dates with more open orders | the same count by `delivery_date`, ordered descending, top 3 with ties |
| Number of open pending items by product | `current_order_items` joined to open `current_orders`, grouped by `product_id`: item count and total quantity |
| Top 3 customers with more pending orders | open `current_orders` by `customer_id`, with the name from the current `dim_customer`, top 3 with ties |

For example, question 2:

```sql
SELECT o.delivery_date, count(*) AS open_orders
FROM gold.current_orders AS o
JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
WHERE s.is_open AND NOT o.is_deleted
GROUP BY o.delivery_date
ORDER BY open_orders DESC
FETCH FIRST 3 ROWS WITH TIES;
```

## Known source issues

- The data generator looks up "today's order of a customer" by customer and date only when inserting items, so new items can be attached to an order of the same day that is already `COMPLETED`. Gold keeps these items; they are not pending because their order is not open.
- `products.unity_price` is a `DECIMAL` without precision or scale, so `unit_price` is an unconstrained `NUMERIC` in gold, which keeps every price exact.
