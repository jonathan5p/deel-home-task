# Querying the lake

The lake can be queried without PostgreSQL, from Spark SQL (`make spark-sql`) or from any engine that speaks the Iceberg REST catalog API, such as DuckDB. It holds the bronze and silver tables, the silver `_history` and `_current` views, and the gold model as Iceberg views. Every table and view, with its columns, is in the [table reference](../reference/index.md).

## The lake gold views

`lake.gold` holds one Iceberg view per public gold relation, computed at read time from silver over the full history. Their definitions and rules are in [Gold in the lake](../lakehouse/gold/writer.md#gold-in-the-lake), and how they compare with PostgreSQL gold in [Gold](../lakehouse/gold/index.md#postgresql-gold-and-the-lake-gold-views).

- **Same columns as the PostgreSQL `gold` views, without the surrogate keys:** facts refer to the dimensions by their natural keys (`customer_id`, `product_id`) and carry the attributes resolved as of each change (`unit_price` and `line_amount`); the dimension views have `_valid_from`, `_valid_to` and `_is_current` for point-in-time joins.

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

DuckDB reads the lake's tables only. The lake's Iceberg views (the silver `_history` and `_current` views and `lake.gold.*`) are stored in Spark SQL, which DuckDB cannot run, so in DuckDB write the equivalent SQL over the silver tables.

The S3 secret above uses the lake's admin key: the REST catalog is read-only, but S3 access with that key is not.

