from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common.settings import AnalyticsDbSettings
from pipelines.bronze.entities import Entity
from pipelines.gold.ddl import PIPELINE_ONLY_COLUMNS, PUBLIC_SCHEMA
from pipelines.gold.dimensions import DIMENSIONS, FACTS
from pipelines.gold.facts import ChangeFact, FactColumn
from pipelines.gold.scd2 import Scd2Column, Scd2Dimension
from pipelines.gold.sink import POSTGRES_DRIVER
from pipelines.lake.checkpoints import (
    CheckpointFiles,
    CommittedBatch,
    HadoopCheckpointFiles,
    committed_batch,
    planned_batch,
)
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.snapshots import read_snapshot
from pipelines.quality.checks import Check, CheckResult
from pipelines.settings import GoldQualitySettings
from pipelines.silver.views import current_query, history_query

SAMPLE_SIZE = 10
METRICS = ("silver_rows", "gold_rows", "missing", "extra", "different", "snapshot_id")
DERIVED_VALIDITY = (MetadataColumn.VALID_TO, MetadataColumn.IS_CURRENT)
VALIDITY = (Scd2Column.VALID_FROM, *DERIVED_VALIDITY)

GOLD_MATCHES_SILVER = Check(
    "gold_matches_silver",
    "The gold view holds the rows silver's views give at the silver snapshot of the gold job's"
    " last committed micro-batch, or of the one it is writing.",
)
NOT_CHECKED = Check(
    "gold_matches_silver",
    "Not checked: the gold stream has committed no micro-batch, so no silver snapshot is known"
    " to compare with.",
)


type BatchReader = Callable[[str], CommittedBatch | None]
type SnapshotReader = Callable[[SparkSession, str, int], DataFrame]
type GoldReader = Callable[[SparkSession, str], DataFrame]
type Samples = Mapping[str, tuple[Mapping[str, object], ...]]


class GoldShape(StrEnum):
    """How a gold view shows silver's versions of a row."""

    VERSIONS = "versions"
    CHANGES = "changes"
    CURRENT = "current"


class UnexpectedSourcesError(ValueError):
    def __init__(self, batch: CommittedBatch) -> None:
        super().__init__(
            f"batch {batch.batch} of query {batch.query_id} has {len(batch.sources)} sources;"
            " a gold stream reads exactly one silver table"
        )


@dataclass(frozen=True, slots=True)
class GoldComparison:
    """A public gold view and the silver rows it must hold."""

    view: str
    stream: str
    entity: Entity
    shape: GoldShape
    key: tuple[str, ...]
    columns: Mapping[str, str]

    @property
    def relation(self) -> str:
        return f"{PUBLIC_SCHEMA}.{self.view}"


def _public(columns: Iterable[str], sources: Mapping[str, str]) -> dict[str, str]:
    return {
        column: sources[column]
        for column in columns
        if column not in PIPELINE_ONLY_COLUMNS and column in sources
    }


def dimension_comparison(dimension: Scd2Dimension) -> GoldComparison:
    """The SCD2 dimension view against silver's live versions."""
    sources = dict(dimension.silver_columns) | {column: column for column in VALIDITY}
    return GoldComparison(
        view=dimension.name,
        stream=dimension.name,
        entity=Entity(dimension.entity),
        shape=GoldShape.VERSIONS,
        key=(dimension.key, Scd2Column.VALID_FROM),
        columns=_public([*(c.name for c in dimension.table.columns), *DERIVED_VALIDITY], sources),
    )


def fact_comparisons(fact: ChangeFact) -> tuple[GoldComparison, GoldComparison]:
    """The history view against silver's history view, and the current-state view against
    silver's current view."""
    entity = Entity(fact.entity)
    history = [*(column.name for column in fact.history.columns), *DERIVED_VALIDITY]
    current = [
        column.name for column in fact.current.columns if column.name != FactColumn.CHANGED_AT
    ]
    return (
        GoldComparison(
            view=fact.history.name,
            stream=fact.name,
            entity=entity,
            shape=GoldShape.CHANGES,
            key=(fact.key, FactColumn.CHANGED_AT),
            columns=_public(history, dict(fact.silver_columns) | {c: c for c in DERIVED_VALIDITY}),
        ),
        GoldComparison(
            view=fact.current.name,
            stream=fact.name,
            entity=entity,
            shape=GoldShape.CURRENT,
            key=(fact.key,),
            columns=_public(current, fact.silver_columns),
        ),
    )


COMPARISONS = (
    *(dimension_comparison(dimension) for dimension in DIMENSIONS),
    *(comparison for fact in FACTS for comparison in fact_comparisons(fact)),
)


@dataclass(frozen=True, slots=True)
class GoldReaders:
    """Where the check reads what the gold job committed, the silver snapshots, and gold."""

    committed: BatchReader
    planned: BatchReader
    read_gold: GoldReader
    read_snapshot: SnapshotReader


class Diff(NamedTuple):
    silver_rows: int
    gold_rows: int
    missing: int
    extra: int
    different: int
    samples: Samples

    @property
    def passed(self) -> bool:
        return self.missing == self.extra == self.different == 0


def gold_read_options(settings: AnalyticsDbSettings, relation: str) -> Mapping[str, str]:
    return {
        "url": settings.url,
        "user": settings.user,
        "password": settings.password,
        "driver": POSTGRES_DRIVER,
        "dbtable": relation,
    }


def jdbc_gold_reader(settings: AnalyticsDbSettings) -> GoldReader:
    def read(spark: SparkSession, relation: str) -> DataFrame:
        return spark.read.format("jdbc").options(**gold_read_options(settings, relation)).load()

    return read


def checkpoint_readers(files: CheckpointFiles) -> tuple[BatchReader, BatchReader]:
    """The committed and the planned batch of a checkpoint, read from `files`."""
    return (
        lambda checkpoint: committed_batch(files, checkpoint),
        lambda checkpoint: planned_batch(files, checkpoint),
    )


def snapshot_id(batch: CommittedBatch) -> int:
    if len(batch.sources) != 1:
        raise UnexpectedSourcesError(batch)
    return int(batch.sources[0]["snapshot_id"])


def snapshot_bounds(lower: CommittedBatch, upper: CommittedBatch | None) -> tuple[int, ...]:
    """The silver snapshots gold may be at: the lower one first, and the upper one if it differs."""
    snapshots = [snapshot_id(lower)]
    if upper is not None and snapshot_id(upper) not in snapshots:
        snapshots.append(snapshot_id(upper))
    return tuple(snapshots)


def silver_rows(spark: SparkSession, comparison: GoldComparison, table: DataFrame) -> DataFrame:
    """The rows the gold view must hold at a silver snapshot."""
    snapshot = f"gold_quality_{comparison.view}"
    table.createOrReplaceTempView(snapshot)
    rows = spark.sql(history_query(comparison.entity, snapshot))
    deleted = F.col(Scd2Column.IS_DELETED)
    if comparison.shape is GoldShape.CURRENT:
        rows.createOrReplaceTempView(f"{snapshot}_history")
        rows = spark.sql(current_query(comparison.entity, f"{snapshot}_history"))
    else:
        rows = rows.withColumn(
            MetadataColumn.IS_CURRENT, F.col(MetadataColumn.IS_CURRENT) & ~deleted
        )
        if comparison.shape is GoldShape.VERSIONS:
            rows = rows.where(~deleted)
    return rows.select(*(F.col(silver).alias(gold) for gold, silver in comparison.columns.items()))


def aligned(gold: DataFrame, schema: StructType) -> DataFrame:
    """The gold rows with silver's column types."""
    return gold.select(*(F.col(field.name).cast(field.dataType) for field in schema.fields))


def _plain(value: object) -> object:
    """A key value as a report can print it as JSON: times in ISO 8601."""
    return value.isoformat() if isinstance(value, datetime | date) else value


def _sample(rows: DataFrame, key: Sequence[str], size: int) -> tuple[Mapping[str, object], ...]:
    keys = rows.select(*key).distinct().orderBy(*key).limit(size).collect()
    return tuple({name: _plain(row[name]) for name in key} for row in keys)


def diff(silver: DataFrame, gold: DataFrame, key: Sequence[str], sample_size: int) -> Diff:
    """Compare the rows as multisets: a row of one side with no equal row on the other is
    `missing` from gold or `extra` in it, unless the other side has an unequal row with the same
    key, in which case the key is `different`."""
    key = list(key)
    silver_only = silver.exceptAll(gold)
    gold_only = gold.exceptAll(silver)
    silver_keys = silver_only.select(*key).distinct()
    gold_keys = gold_only.select(*key).distinct()
    missing = silver_only.join(gold_keys, key, "left_anti")
    extra = gold_only.join(silver_keys, key, "left_anti")
    different = silver_keys.join(gold_keys, key, "inner")
    return Diff(
        silver_rows=silver.count(),
        gold_rows=gold.count(),
        missing=missing.count(),
        extra=extra.count(),
        different=different.count(),
        samples={
            "missing": _sample(missing, key, sample_size),
            "extra": _sample(extra, key, sample_size),
            "different": _sample(different, key, sample_size),
        },
    )


def _result(comparison: GoldComparison, found: Diff, snapshot: int) -> CheckResult:
    metrics = {
        "silver_rows": found.silver_rows,
        "gold_rows": found.gold_rows,
        "missing": found.missing,
        "extra": found.extra,
        "different": found.different,
        "snapshot_id": snapshot,
    }
    return GOLD_MATCHES_SILVER.result(
        comparison.relation, passed=found.passed, metrics=metrics, details=found.samples
    )


def check_view(
    spark: SparkSession,
    settings: GoldQualitySettings,
    comparison: GoldComparison,
    readers: GoldReaders,
    sample_size: int = SAMPLE_SIZE,
) -> CheckResult:
    """Compare a gold view with silver at the snapshots gold may be at."""
    checkpoint = settings.checkpoint_path(comparison.stream)
    committed = readers.committed(checkpoint)
    if committed is None:
        metrics = dict.fromkeys(METRICS)
        return NOT_CHECKED.result(comparison.relation, passed=False, metrics=metrics)
    gold = (
        readers.read_gold(spark, comparison.relation)
        .select(*comparison.columns)
        .localCheckpoint(eager=True)
    )
    newest = readers.planned(checkpoint) or readers.committed(checkpoint)
    table = settings.source_table(comparison.entity.name)

    def at(snapshot: int) -> CheckResult:
        silver = silver_rows(spark, comparison, readers.read_snapshot(spark, table, snapshot))
        found = diff(silver, aligned(gold, silver.schema), comparison.key, sample_size)
        return _result(comparison, found, snapshot)

    checks = (at(snapshot) for snapshot in snapshot_bounds(committed, newest))
    first = next(checks)
    if first.passed:
        return first
    return next((check for check in checks if check.passed), first)


def check_gold(
    spark: SparkSession,
    settings: GoldQualitySettings,
    readers: GoldReaders,
    comparisons: Iterable[GoldComparison] = COMPARISONS,
) -> list[CheckResult]:
    return [check_view(spark, settings, comparison, readers) for comparison in comparisons]


def run_gold_checks(
    spark: SparkSession,
    env: Mapping[str, str],
    *,
    files: CheckpointFiles | None = None,
    read_gold: GoldReader | None = None,
    read_snapshot: SnapshotReader = read_snapshot,
) -> list[CheckResult]:
    """Check every public gold view against silver at what the gold job has committed."""
    settings = GoldQualitySettings.from_env(env)
    committed, planned = checkpoint_readers(files or HadoopCheckpointFiles(spark))
    readers = GoldReaders(
        committed=committed,
        planned=planned,
        read_gold=read_gold or jdbc_gold_reader(settings.analytics_db),
        read_snapshot=read_snapshot,
    )
    return check_gold(spark, settings, readers)
