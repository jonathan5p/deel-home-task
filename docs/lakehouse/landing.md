# Landing

The landing job (`analytics/pipelines/src/pipelines/lake/landing/job.py`) streams every record of the CDC topics into raw Avro files, exactly as Kafka delivered it, so the lake keeps a replayable copy of the sources:

```sh
make pipeline PIPELINE=landing   # as a long-running service; see Running the pipeline below
make spark-submit SPARK_APP=analytics/pipelines/src/pipelines/lake/landing/job.py SPARK_CONF="spark.cores.max=2" SPARK_UI_PORT=4050   # or ad hoc, in the foreground
```

- **One streaming query per topic,** reading from the earliest offset and writing to `<LAKE_ROOT>/landing/<topic>/`: one folder per topic, with no partition folders.
- **Each record is stored unchanged:** `key` and `value` as raw bytes, plus `topic`, `partition`, `offset`, `timestamp` and `timestampType`. Tombstones keep their `null` value.
- **Restart-safe:** each query keeps its checkpoint in `<CHECKPOINT_ROOT>/landing/<topic>/`, and Spark's file sink records the committed files in `_spark_metadata`. Stopping and restarting the job neither skips nor duplicates records; Spark readers of the folder see only committed files.
- **Micro-batches** run every `LANDING_TRIGGER_SECONDS`. `spark.cores.max` keeps cores free for the other jobs; the job runs until it is stopped.

The landing zone's place in the data flow is in the [architecture overview](../architecture/overview.md#data-flow), and its checks against Kafka in [data quality](data-quality.md).

## Inspecting the landing zone with Spark SQL

`make spark-sql` opens the Spark SQL shell in its own driver container, like `make spark-submit`: it runs on the cluster, reads the lake with the S3A credentials, and can use the Iceberg catalog `lake` (Spark loads it on first use, so it appears in `SHOW CATALOGS` only after a query such as `SHOW NAMESPACES IN lake;`). It uses Spark's in-memory session catalog, so temporary views live for the session only.

```sh
make spark-sql                                                      # interactive shell
make spark-sql SPARK_SQL_FILE=tools/spark-sql/landing_examples.sql \
  SPARK_SQL_VARS="topic=finance_db.operations.orders key_field=order_id key_value=1 from_offset=0 to_offset=99"
```

[`tools/spark-sql/landing_examples.sql`](https://github.com/jonathan5p/deel-home-task/blob/main/tools/spark-sql/landing_examples.sql) inspects one topic at a time and contains no topic or field names. Its variables:

| Variable | Set with | Meaning |
|---|---|---|
| `landing_root` | `LANDING_ROOT` (default `s3a://lake/landing`) | Root of the landing zone |
| `topic` | `SPARK_SQL_VARS` | Topic folder to inspect |
| `key_field`, `key_value` | `SPARK_SQL_VARS` | Key field and value for the `by key` example |
| `from_offset`, `to_offset` | `SPARK_SQL_VARS` | Offset range for the `offset range` example (both ends included) |

It defines a `landing` view that decodes the topic's records: the source coordinates (`record_source`, `source_partition`, `source_offset`, `published_at`), the key as JSON, `change_op` (`create`, `update`, `delete`, `snapshot`), and `before_json`/`after_json`. Tombstones have `tombstone = true` and no operation. Its examples return:

| Example | Returns |
|---|---|
| `latest` | the latest records |
| `by key` | every change of one key, in log order |
| `offset range` | an offset range |
| `last hour` | records published in the last hour |
| `counts` | record counts by operation |
| `tombstones` | tombstone records |

When a whole file runs, the result sets follow each other without headers, so every example's first column is its name. In the shell, set the variables first (for example `SET hivevar:topic=finance_db.operations.orders;`), then paste the views and the queries you need.

The default landing root only has data once the landing job has run; until then the views fail with `PATH_NOT_FOUND`.

Spark reads each topic folder through its file-sink commit log (`_spark_metadata`), so files from failed micro-batches are ignored and counts are exact.

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.
