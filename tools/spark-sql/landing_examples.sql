-- Debugging queries for the landing zone. Run them all with
--   make spark-sql SPARK_SQL_FILE=tools/spark-sql/landing_examples.sql
-- or open the shell with `make spark-sql` and paste the views and the queries you need.
-- ${landing_root} comes from LANDING_ROOT (default s3a://lake/landing).
-- The first column of every example names it, so the results of a whole-file run stay readable.

CREATE OR REPLACE TEMPORARY VIEW landing_customers
USING avro OPTIONS (path '${landing_root}/finance_db.operations.customers');
CREATE OR REPLACE TEMPORARY VIEW landing_products
USING avro OPTIONS (path '${landing_root}/finance_db.operations.products');
CREATE OR REPLACE TEMPORARY VIEW landing_orders
USING avro OPTIONS (path '${landing_root}/finance_db.operations.orders');
CREATE OR REPLACE TEMPORARY VIEW landing_order_items
USING avro OPTIONS (path '${landing_root}/finance_db.operations.order_items');

-- Every landing record with its source coordinates and the decoded Debezium envelope.
CREATE OR REPLACE TEMPORARY VIEW landing AS
SELECT
    topic AS record_source,
    partition AS source_partition,
    offset AS source_offset,
    timestamp AS published_at,
    cast(key AS STRING) AS key_json,
    value IS NULL AS tombstone,
    CASE get_json_object(cast(value AS STRING), '$.op')
        WHEN 'c' THEN 'create'
        WHEN 'u' THEN 'update'
        WHEN 'd' THEN 'delete'
        WHEN 'r' THEN 'snapshot'
    END AS change_op,
    get_json_object(cast(value AS STRING), '$.ts_ms') AS captured_at_ms,
    get_json_object(cast(value AS STRING), '$.before') AS before_json,
    get_json_object(cast(value AS STRING), '$.after') AS after_json
FROM (
    SELECT * FROM landing_customers
    UNION ALL SELECT * FROM landing_products
    UNION ALL SELECT * FROM landing_orders
    UNION ALL SELECT * FROM landing_order_items
);

-- Latest records of one topic.
SELECT 'latest' AS example, record_source, source_partition, source_offset, published_at, key_json, change_op
FROM landing
WHERE record_source = 'finance_db.operations.orders'
ORDER BY source_offset DESC
LIMIT 10;

-- Every change of one key, in log order.
SELECT 'by key' AS example, record_source, source_partition, source_offset, published_at, change_op, after_json
FROM landing
WHERE record_source = 'finance_db.operations.orders'
  AND get_json_object(key_json, '$.order_id') = '1'
ORDER BY source_partition, source_offset;

-- An offset range of one partition (both ends included).
SELECT 'offset range' AS example, record_source, source_partition, source_offset, published_at, change_op
FROM landing
WHERE record_source = 'finance_db.operations.orders'
  AND source_partition = 0
  AND source_offset BETWEEN 1 AND 3
ORDER BY source_offset;

-- Records published to Kafka in the last hour.
SELECT 'last hour' AS example, record_source, source_partition, source_offset, published_at, change_op
FROM landing
WHERE published_at >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY published_at, record_source, source_offset;

-- Record counts by topic and operation; tombstones have no operation.
SELECT 'counts' AS example, record_source, coalesce(change_op, 'tombstone') AS change_op, count(*) AS records
FROM landing
GROUP BY record_source, coalesce(change_op, 'tombstone')
ORDER BY record_source, change_op;

-- Tombstones: a key followed by a null value after a delete.
SELECT 'tombstones' AS example, record_source, source_partition, source_offset, key_json
FROM landing
WHERE tombstone
ORDER BY record_source, source_partition, source_offset;
