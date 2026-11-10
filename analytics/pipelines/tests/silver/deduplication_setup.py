from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession

from analytics.pipelines.tests.silver.versions_samples import (
    CHANGED_AT,
    BronzeChange,
    bronze_change,
    bronze_frame,
    bronze_schema,
)
from pipelines.bronze.entities import CUSTOMERS
from pipelines.lake.columns import ChangeOp, MetadataColumn

ENTITY = CUSTOMERS
KEY = ENTITY.lake_key

type Transform = Callable[[DataFrame], DataFrame]
type ChangeIdentity = tuple[int, int, str]


def change(sequence: int, op: ChangeOp, *, key: int = 1, values: int | None = None) -> BronzeChange:
    """A customers change of `key` committed at `sequence`, with the values `bronze_change`
    derives from `values` (by default from `sequence`)."""
    sample = bronze_change(ENTITY, sequence if values is None else values, op)
    return sample | {KEY: key, MetadataColumn.COMMIT_SEQUENCE: sequence}


def redelivered(original: BronzeChange, offset: int) -> BronzeChange:
    """`original` delivered again, at another Kafka offset and capture time."""
    later = CHANGED_AT + timedelta(minutes=offset)
    return original | {
        MetadataColumn.SOURCE_OFFSET: offset,
        MetadataColumn.PUBLISHED_AT: later,
        MetadataColumn.CAPTURED_AT: later,
    }


def run_per_batch(
    spark: SparkSession, transform: Transform, batches: list[list[BronzeChange]], folder: Path
) -> list[list[Row]]:
    """Stream the bronze rows of each batch through `transform`, one restart (and one micro-batch)
    per batch on the same checkpoint, and return the rows each run appends."""
    source = folder / "bronze"
    source.mkdir(parents=True)
    appended: list[list[Row]] = []
    for changes in batches:
        bronze_frame(spark, ENTITY, changes).coalesce(1).write.mode("append").parquet(str(source))
        rows: list[Row] = []

        def collect(frame: DataFrame, _batch_id: int, rows: list[Row] = rows) -> None:
            rows.extend(frame.collect())

        stream = transform(spark.readStream.schema(bronze_schema(ENTITY)).parquet(str(source)))
        query = (
            stream.writeStream.foreachBatch(collect)
            .option("checkpointLocation", str(folder / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        appended.append(rows)
    return appended


def identities(rows: list[Row]) -> list[ChangeIdentity]:
    """The key, commit sequence and operation of each row, sorted."""
    return sorted(
        (row[KEY], row[MetadataColumn.COMMIT_SEQUENCE], row[MetadataColumn.CHANGE_OP])
        for row in rows
    )
