# Querying the lake

The lake can be queried without PostgreSQL, from Spark SQL (`make spark-sql`) or from any engine that speaks the Iceberg REST catalog API, such as DuckDB. It holds the bronze and silver tables, the silver `_history` and `_current` views, the gold tables the gold job writes, and the gold views over them. Every table and view, with its columns, is in the [table reference](../reference/index.md).

## The lake gold views

The lake holds gold with the same split as PostgreSQL: the Iceberg tables the gold job writes, `lake.gold_internal.<table>`, with every column it stores and the full history, and in `lake.gold` the same 14 views as PostgreSQL's `gold` schema over them, rendered from the same SQL templates. How they are written is in [Gold in the lake](../lakehouse/gold/writer.md#gold-in-the-lake), and how they compare with PostgreSQL gold in [Gold](../lakehouse/gold/index.md#postgresql-gold-and-the-lake-gold-views).

- **Same SQL and columns as the PostgreSQL `gold` views, surrogate keys included:** the point-in-time fact views resolve the dimension version valid at each change, and its key is hashed from the version's business key and `_commit_sequence`, so it has the same value in the lake as in PostgreSQL. The four business-question views count the open current orders when read, as in PostgreSQL.
- **Full history:** PostgreSQL's retention window does not apply to the lake.
- **As fresh as the gold tables:** the views read the Iceberg tables, which the gold job commits just before it copies each batch into PostgreSQL.
- **Consistent per table:** each table commits on its own, so for a moment a reader can see a fact's new history before its current state ([consistency](../lakehouse/gold/writer.md#consistency-between-the-two-copies)).

```sql
-- Question 2 of the take-home, from the lake: the top 3 delivery dates by open orders, ties included
SELECT delivery_date, open_orders
FROM lake.gold.top_delivery_dates_by_open_orders
ORDER BY open_orders DESC, delivery_date;
```

## Query the lake from DuckDB

Start the read-only [Iceberg REST catalog](../architecture/infrastructure.md#iceberg-rest-catalog) with `make up PROFILES=catalog`, which also creates its login (`make db-logins`), so it works on an existing volume too.
Then, in DuckDB 2.0 or later, attach it:

```sql
INSTALL iceberg;
LOAD iceberg;
CREATE SECRET lake_s3 (
    TYPE s3, KEY_ID 'lakeadmin', SECRET 'lakeadmin-secret',
    ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'
);
ATTACH 'warehouse' AS lake (TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');

SHOW ALL TABLES;
SELECT _change_op, count(*) AS changes FROM lake.bronze.orders GROUP BY _change_op;

-- Live orders by status: the latest version of each order, unless it was deleted.
SELECT status, count(*) AS orders
FROM (
    SELECT * FROM lake.silver.orders
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _commit_sequence DESC) = 1
)
WHERE NOT _is_deleted
GROUP BY status;
```

DuckDB 1.x takes `ENDPOINT 'http://localhost:8181'` instead of `URI`.

DuckDB reads the lake's tables only. The lake's Iceberg views (the silver `_history` and `_current` views and the `lake.gold` views) are stored as Spark SQL, which DuckDB does not run from the catalog. For gold, run the views' own SQL over the tables: every view is a template in [`pipelines/lake/gold/sql/views/`](https://github.com/jonathan5p/deel-home-task/tree/main/analytics/pipelines/src/pipelines/lake/gold/sql/views), written in plain SQL, with `{{ internal }}` standing for the namespace of the tables (`lake.gold_internal` here). For example, the top 3 delivery dates and the current customer versions:

```sql
SELECT delivery_date, open_orders
FROM (
    SELECT delivery_date, count(*) AS open_orders, rank() OVER (ORDER BY count(*) DESC) AS place
    FROM lake.gold_internal.current_orders
    WHERE is_open AND NOT _is_deleted
    GROUP BY delivery_date
)
WHERE place <= 3
ORDER BY open_orders DESC, delivery_date;

SELECT customer_key, customer_id, customer_name, _valid_from
FROM lake.gold_internal.dim_customer
WHERE _is_current;
```

The dimension views leave out versions that are folded or deleted (`NOT _is_folded AND NOT _is_deleted`), the current-state views leave out deleted rows, and the history views leave out folded changes and join the dimension version valid at each change (`fact_order_history.sql.j2`, `fact_order_item_history.sql.j2`). For silver, write the equivalent SQL over the silver tables.

The S3 secret above uses the lake's admin key: the REST catalog is read-only, but S3 access with that key is not.

