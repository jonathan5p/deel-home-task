from collections.abc import Callable
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pipelines.lake.bronze.entities import Entity, entity_for_topic
from pipelines.lake.shared.checkpoints import (
    CheckpointFiles,
    CommittedBatch,
    HadoopCheckpointFiles,
    committed_batch,
)
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.snapshots import (
    Snapshot,
    committed_snapshot,
    read_snapshot,
    table_snapshots,
)
from pipelines.quality.checks import Check, CheckResult
from pipelines.settings import SilverSettings

type SnapshotReader = Callable[[SparkSession, str], list[Snapshot]]
type Sample = tuple[dict[str, object], ...]

KEY = "key"
IDENTITY = StructType(
    [
        StructField(KEY, LongType()),
        StructField(MetadataColumn.COMMIT_SEQUENCE, LongType()),
        StructField(MetadataColumn.CHANGE_OP, StringType()),
    ]
)
SAMPLE_SIZE = 10

SAME_CHANGES_AS_BRONZE = Check(
    "same_changes_as_bronze",
    "Silver holds exactly the changes (key, commit sequence, change op) bronze held at the"
    " snapshot silver's committed batch had read.",
)
CHANGES_HELD_ONCE = Check(
    "changes_held_once",
    "No change (key, commit sequence, change op) is in silver more than once.",
)


class Committed(NamedTuple):
    """What the entity's silver stream has committed: the changes bronze held at the snapshot its
    committed batch had read (None when that snapshot no longer exists), the changes silver holds
    up to that batch, and the batch."""

    expected: DataFrame | None
    actual: DataFrame
    batch: CommittedBatch | None
    bronze_snapshot: int | None = None
    silver_snapshot: int | None = None


def no_changes(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], IDENTITY)


def changes(rows: DataFrame, entity: Entity) -> DataFrame:
    return rows.select(
        F.col(entity.lake_key).cast(LongType()).alias(KEY),
        F.col(MetadataColumn.COMMIT_SEQUENCE).cast(LongType()),
        F.col(MetadataColumn.CHANGE_OP),
    )


def committed(
    spark: SparkSession,
    settings: SilverSettings,
    entity: Entity,
    files: CheckpointFiles,
    snapshots: SnapshotReader,
) -> Committed:
    batch = committed_batch(files, settings.checkpoint_path(entity.name))
    if batch is None:
        return Committed(no_changes(spark), no_changes(spark), None)
    bronze = settings.source_table(entity.name)
    bronze_snapshot = int(batch.sources[0]["snapshot_id"])
    bronze_exists = any(
        snapshot.snapshot_id == bronze_snapshot for snapshot in snapshots(spark, bronze)
    )
    expected = (
        changes(read_snapshot(spark, bronze, bronze_snapshot), entity) if bronze_exists else None
    )
    silver = settings.table(entity.name)
    silver_snapshot = committed_snapshot(snapshots(spark, silver), batch)
    actual = (
        no_changes(spark)
        if silver_snapshot is None
        else changes(read_snapshot(spark, silver, silver_snapshot), entity)
    )
    return Committed(expected, actual, batch, bronze_snapshot, silver_snapshot)


def sample(rows: DataFrame) -> Sample:
    """Up to `SAMPLE_SIZE` change identities, in order."""
    ordered = rows.orderBy(*IDENTITY.fieldNames()).limit(SAMPLE_SIZE).collect()
    return tuple(row.asDict() for row in ordered)


def same_changes(topic: str, silver: Committed) -> CheckResult:
    batch = {"silver_batch": None if silver.batch is None else silver.batch.batch}
    details: dict[str, object] = {
        "bronze_snapshot": silver.bronze_snapshot,
        "silver_snapshot": silver.silver_snapshot,
    }
    if silver.expected is None:
        details["reason"] = (
            f"bronze snapshot {silver.bronze_snapshot}, which silver's committed batch had read,"
            " no longer exists"
        )
        return SAME_CHANGES_AS_BRONZE.result(
            topic,
            passed=False,
            metrics={"bronze_changes": None, "missing": None, "extra": None} | batch,
            details=details,
        )
    missing = silver.expected.subtract(silver.actual)
    extra = silver.actual.subtract(silver.expected)
    counts = {"missing": missing.count(), "extra": extra.count()}
    details |= {
        name: sample(rows)
        for name, rows in (("missing", missing), ("extra", extra))
        if counts[name]
    }
    return SAME_CHANGES_AS_BRONZE.result(
        topic,
        passed=not any(counts.values()),
        metrics={"bronze_changes": silver.expected.distinct().count()} | counts | batch,
        details=details,
    )


def held_once(topic: str, silver: Committed) -> CheckResult:
    duplicated = (
        silver.actual.groupBy(*IDENTITY.fieldNames()).count().where("count > 1").drop("count")
    )
    duplicates = duplicated.count()
    return CHANGES_HELD_ONCE.result(
        topic,
        passed=duplicates == 0,
        metrics={"silver_rows": silver.actual.count(), "duplicated": duplicates},
        details={"duplicated": sample(duplicated)} if duplicates else None,
    )


def reconcile(topic: str, silver: Committed) -> list[CheckResult]:
    return [same_changes(topic, silver), held_once(topic, silver)]


def run_checks(
    spark: SparkSession,
    settings: SilverSettings,
    *,
    files: CheckpointFiles | None = None,
    snapshots: SnapshotReader = table_snapshots,
) -> list[CheckResult]:
    files = files or HadoopCheckpointFiles(spark)
    results = []
    for topic in settings.topics:
        entity = entity_for_topic(topic)
        results += reconcile(topic, committed(spark, settings, entity, files, snapshots))
    return results
