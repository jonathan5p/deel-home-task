from datetime import datetime

from pyspark.sql import DataFrame, Row, SparkSession

from analytics.pipelines.tests.bronze.job_setup import record
from analytics.pipelines.tests.bronze.parse_samples import envelope, source_row
from pipelines.bronze.entities import Entity
from pipelines.bronze.job import run
from pipelines.bronze.replay import OffsetRange, ReplayRequest, ReplaySummary, replay
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.streams import await_queries
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

FIRST_LSN = 500


def created(entity: Entity, offset: int, *, lsn: int | None = None) -> Row:
    """A valid create whose change identity is unique to its offset unless `lsn` repeats one."""
    value = envelope(
        "c", after=source_row(entity), lsn=lsn if lsn is not None else FIRST_LSN + offset
    )
    return record(entity, offset, value)


def invalid(entity: Entity, offset: int) -> Row:
    return record(entity, offset, "not json")


def placed(row: Row, *, partition: int = 0, timestamp: datetime | None = None) -> Row:
    """`row` moved to another partition or publish time."""
    fields = row.asDict()
    fields["partition"] = partition
    if timestamp is not None:
        fields["timestamp"] = timestamp
    return Row(**fields)


def kafka_records(spark: SparkSession, rows: list[Row]) -> DataFrame:
    return spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA)


def offsets_of(frame: DataFrame) -> list[int]:
    return sorted(row[MetadataColumn.SOURCE_OFFSET] for row in frame.collect())


def run_bronze(spark: SparkSession, settings: BronzeSettings) -> None:
    await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)


def replay_offsets(
    spark: SparkSession, settings: BronzeSettings, topic: str, first: int, last: int
) -> ReplaySummary:
    return replay(spark, settings, ReplayRequest(topic, OffsetRange(first, last)))
