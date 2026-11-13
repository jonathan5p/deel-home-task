# Querying the lake

The lake can be queried from Spark SQL (`make spark-sql`) or any engine that speaks the Iceberg REST catalog API, such as DuckDB. It holds the bronze and silver tables and views, and the gold tables and views ([table reference](../reference/index.md)).

## The lake gold views

`lake.gold_internal.<table>` holds the gold Iceberg tables with the full history, and `lake.gold` the same 14 views as PostgreSQL's `gold` schema, from the same templates ([gold in the lake](../lakehouse/gold/writer.md#gold-in-the-lake), [compared with PostgreSQL](../lakehouse/gold/index.md#postgresql-gold-and-the-lake-gold-views)).

- **Same SQL, columns and surrogate keys** as the PostgreSQL views.
- **Full history:** PostgreSQL's retention does not apply.
- **As fresh as the tables,** which the gold job commits just before the PostgreSQL copy.
- **Consistent per table:** a reader can briefly see a fact's new history before its current state ([consistency](../lakehouse/gold/writer.md#consistency-between-the-two-copies)).

```sql
-- Question 2 of the take-home, from the lake: the top 3 delivery dates by open orders, ties included
SELECT delivery_date, open_orders
FROM lake.gold.top_delivery_dates_by_open_orders
ORDER BY open_orders DESC, delivery_date;
```

## Query the lake from DuckDB

Start the read-only [Iceberg REST catalog](../architecture/infrastructure.md#iceberg-rest-catalog) with `make up PROFILES=catalog`, then attach it in DuckDB 2.0 or later:

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

DuckDB reads the tables only: the lake's views are stored as Spark SQL. For gold, run the views' own SQL over the tables: each view is a plain-SQL template in [`pipelines/lake/gold/sql/views/`](https://github.com/jonathan5p/deel-home-task/tree/main/analytics/pipelines/src/pipelines/lake/gold/sql/views), with `{{ internal }}` standing for `lake.gold_internal`. For example:

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

The dimension views leave out folded and deleted versions, the current-state views deleted rows, and the history views folded changes. For silver, write the equivalent SQL over the silver tables.

The S3 secret above uses the lake's admin key: the REST catalog is read-only, but S3 access with that key is not.

