-- Debugging queries for one landing topic. Run them all with
--   make spark-sql SPARK_SQL_FILE=tools/spark-sql/landing_examples.sql \
--     SPARK_SQL_VARS="topic=<topic> key_field=<key field> key_value=<key value> from_offset=<n> to_offset=<m>"
-- ${landing_root} comes from LANDING_ROOT; the other variables come from SPARK_SQL_VARS.
-- The first column of every example names it, so the results of a whole-file run stay readable.

CREATE OR REPLACE TEMPORARY VIEW landing_topic
USING avro OPTIONS (path '${landing_root}/${topic}');

-- Every record of the topic with its source coordinates and the decoded Debezium envelope.
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
FROM landing_topic;

-- Latest records.
SELECT 'latest' AS example, record_source, source_partition, source_offset, published_at, key_json, change_op
FROM landing
ORDER BY source_offset DESC
LIMIT 10;

-- Every change of one key, in log order.
SELECT 'by key' AS example, record_source, source_partition, source_offset, published_at, change_op, after_json
FROM landing
WHERE get_json_object(key_json, '$.${key_field}') = '${key_value}'
ORDER BY source_partition, source_offset;

-- An offset range (both ends included).
SELECT 'offset range' AS example, record_source, source_partition, source_offset, published_at, change_op
FROM landing
WHERE source_offset BETWEEN ${from_offset} AND ${to_offset}
ORDER BY source_partition, source_offset;

-- Records published in the last hour.
SELECT 'last hour' AS example, record_source, source_partition, source_offset, published_at, change_op
FROM landing
WHERE published_at >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY published_at, source_partition, source_offset;

-- Record counts by operation; tombstones have no operation.
SELECT 'counts' AS example, record_source, coalesce(change_op, 'tombstone') AS change_op, count(*) AS records
FROM landing
GROUP BY record_source, coalesce(change_op, 'tombstone')
ORDER BY change_op;

-- Tombstones: a key followed by a null value after a delete.
SELECT 'tombstones' AS example, record_source, source_partition, source_offset, key_json
FROM landing
WHERE tombstone
ORDER BY source_partition, source_offset;
