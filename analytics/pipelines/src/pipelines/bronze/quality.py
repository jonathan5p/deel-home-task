from collections.abc import Callable
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from pipelines.bronze.entities import entity_for_topic
from pipelines.lake.checkpoints import (
    CheckpointFiles,
    CommittedBatch,
    HadoopCheckpointFiles,
    committed_batch,
    committed_files,
)
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.snapshots import Snapshot, committed_snapshot, read_snapshot, table_snapshots
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.quality.checks import Check, CheckResult
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

COORDINATES = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
    ]
)

LANDED_RECORDS_HELD = Check(
    "landed_records_held",
    "Every landed record that bronze and its quarantine have both committed is in one of them.",
)
HELD_EXACTLY_ONCE = Check(
    "held_exactly_once",
    "No landed record is in both bronze and its quarantine, or twice in either.",
)
COMMITTED_RECORDS_ONLY = Check(
    "committed_records_only",
    "Bronze and its quarantine hold only records of the landing files their queries committed.",
)

type SnapshotReader = Callable[[SparkSession, str], list[Snapshot]]


class Committed(NamedTuple):
    """What one bronze query, the entity's bronze or quarantine stream, has committed: the
    coordinates of the landing records it read, of its table's rows up to its committed batch,
    and that batch."""

    landed: DataFrame
    held: DataFrame
    batch: CommittedBatch | None


def no_coordinates(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], COORDINATES)


def landed_records(spark: SparkSession, paths: frozenset[str]) -> DataFrame:
    """The coordinates of the landing files' records, without tombstones, which bronze drops."""
    if not paths:
        return no_coordinates(spark)
    records = spark.read.format("avro").schema(KAFKA_RECORD_SCHEMA).load(sorted(paths))
    return records.where(F.col("value").isNotNull()).select(*COORDINATES.fieldNames())


def held_coordinates(rows: DataFrame) -> DataFrame:
    return rows.select(
        F.col(MetadataColumn.RECORD_SOURCE).alias("topic"),
        F.col(MetadataColumn.SOURCE_PARTITION).alias("partition"),
        F.col(MetadataColumn.SOURCE_OFFSET).alias("offset"),
    )


def committed(
    spark: SparkSession,
    settings: BronzeSettings,
    name: str,
    files: CheckpointFiles,
    snapshots: SnapshotReader,
) -> Committed:
    batch = committed_batch(files, settings.checkpoint_path(name))
    if batch is None:
        return Committed(no_coordinates(spark), no_coordinates(spark), None)
    table = settings.table(name)
    paths = committed_files(files, settings.checkpoint_path(name), batch)
    snapshot = committed_snapshot(snapshots(spark, table), batch)
    held = (
        no_coordinates(spark)
        if snapshot is None
        else held_coordinates(read_snapshot(spark, table, snapshot))
    )
    return Committed(landed_records(spark, paths), held, batch)


def reconcile(topic: str, bronze: Committed, quarantine: Committed) -> list[CheckResult]:
    """Check one topic's landing records against bronze and its quarantine.

    The two tables are written by separate queries that commit separately, so a record counts as
    missing only once both have committed its file, and as unexpected when the query that holds
    it never committed its file.
    """
    held = bronze.held.unionByName(quarantine.held)
    both_committed = bronze.landed.intersect(quarantine.landed)
    missing = both_committed.subtract(held).count()
    duplicated = held.groupBy(*COORDINATES.fieldNames()).count().where("count > 1").count()
    unexpected = (
        bronze.held.subtract(bronze.landed).count()
        + quarantine.held.subtract(quarantine.landed).count()
    )
    batches = {
        "bronze_batch": None if bronze.batch is None else bronze.batch.batch,
        "quarantine_batch": None if quarantine.batch is None else quarantine.batch.batch,
    }
    return [
        LANDED_RECORDS_HELD.result(
            topic,
            passed=missing == 0,
            metrics={"committed_records": both_committed.count(), "missing": missing} | batches,
        ),
        HELD_EXACTLY_ONCE.result(
            topic,
            passed=duplicated == 0,
            metrics={"held_records": held.count(), "duplicated": duplicated},
        ),
        COMMITTED_RECORDS_ONLY.result(
            topic, passed=unexpected == 0, metrics={"unexpected": unexpected}
        ),
    ]


def run_checks(
    spark: SparkSession,
    settings: BronzeSettings,
    *,
    files: CheckpointFiles | None = None,
    snapshots: SnapshotReader = table_snapshots,
) -> list[CheckResult]:
    files = files or HadoopCheckpointFiles(spark)
    results = []
    for topic in settings.topics:
        entity = entity_for_topic(topic)
        bronze = committed(spark, settings, entity.name, files, snapshots)
        quarantine_name = TABLE_TEMPLATES["bronze_quarantine"].table_name(entity.definition)
        quarantine = committed(spark, settings, quarantine_name, files, snapshots)
        results += reconcile(topic, bronze, quarantine)
    return results
