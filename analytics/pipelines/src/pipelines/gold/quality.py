from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from itertools import product
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common.settings import AnalyticsDbSettings
from pipelines.bronze.entities import Entity
from pipelines.gold.ddl import PUBLIC_SCHEMA
from pipelines.gold.facts import ChangeFact, FactColumn
from pipelines.gold.lake.views import FRESHNESS, GoldLakeViews
from pipelines.gold.scd2 import INTERNAL_SCHEMA, Scd2Column, Scd2Dimension
from pipelines.gold.sink import jdbc_options
from pipelines.lake.checkpoints import (
    CheckpointFiles,
    CommittedBatch,
    HadoopCheckpointFiles,
    committed_batch,
    planned_batch,
)
from pipelines.lake.snapshots import read_snapshot
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.lake.views import StoredView, ViewDefinition, stored_view
from pipelines.quality.checks import Check, CheckResult
from pipelines.settings import GoldSettings, SilverSettings
from pipelines.silver.views import HISTORY, history_query

SAMPLE_SIZE = 10
TEMP_PREFIX = "gold_check"
METRICS = ("lake_rows", "postgres_rows", "lake_only", "postgres_only", "stale")

GOLD_MATCHES_SILVER = Check(
    "gold_matches_silver",
    "The PostgreSQL gold view holds the rows of the gold view in the lake, computed over silver"
    " at the snapshots of the gold job's last committed micro-batches or of the ones it is"
    " writing, and the lake view is up to date with its definition; surrogate keys are compared"
    " as the dimension versions they name, and history from the first month PostgreSQL keeps.",
)
NOT_CHECKED = Check(
    "gold_matches_silver",
    "Not checked: a gold stream has committed no micro-batch, so no silver snapshot is known"
    " to compare at.",
)


type Sample = tuple[Mapping[str, object], ...]


class Differences(NamedTuple):
    expected_only: DataFrame
    actual_only: DataFrame


def aligned(rows: DataFrame, schema: StructType) -> DataFrame:
    """`rows` with the column names, order and types of `schema`."""
    return rows.select(*(F.col(field.name).cast(field.dataType) for field in schema.fields))


def differences(expected: DataFrame, actual: DataFrame) -> Differences:
    """The rows only one side has, comparing the two as multisets."""
    return Differences(expected.exceptAll(actual), actual.exceptAll(expected))


def plain(value: object) -> object:
    """`value` as a JSON report can hold it."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def sample(rows: DataFrame, columns: Sequence[str], size: int) -> Sample:
    """Up to `size` distinct values of `columns`, in their order."""
    picked = rows.select(*columns).distinct().orderBy(*columns).limit(size).collect()
    return tuple({name: plain(row[name]) for name in columns} for row in picked)


type BatchReader = Callable[[str], CommittedBatch | None]
type SnapshotReader = Callable[[SparkSession, str, int], DataFrame]
type PostgresReader = Callable[[SparkSession, str], DataFrame]
type StoredViewReader = Callable[[SparkSession, str], StoredView | None]
type Snapshots = Mapping[str, int]


class UnexpectedSourcesError(ValueError):
    def __init__(self, checkpoint: str, count: int) -> None:
        super().__init__(
            f"the batch in {checkpoint} has {count} sources; a gold stream reads one silver table"
        )


@dataclass(frozen=True, slots=True)
class GoldReaders:
    committed: BatchReader
    planned: BatchReader
    read_snapshot: SnapshotReader
    read_postgres: PostgresReader
    read_stored_view: StoredViewReader = stored_view


class Bounds(NamedTuple):
    committed: int
    planned: int | None


class Matching(NamedTuple):
    lake_rows: int
    postgres_rows: int
    lake_only: int
    postgres_only: int
    samples: Mapping[str, Sample]

    @property
    def passed(self) -> bool:
        return self.lake_only == self.postgres_only == 0


def snapshot_id(checkpoint: str, batch: CommittedBatch) -> int:
    if len(batch.sources) != 1:
        raise UnexpectedSourcesError(checkpoint, len(batch.sources))
    return int(batch.sources[0]["snapshot_id"])


def snapshot_choices(bounds: Mapping[str, Bounds]) -> list[dict[str, int]]:
    """Every combination of silver snapshots gold may be at, the committed one first."""
    options = [
        [
            bound.committed,
            *([bound.planned] if bound.planned not in (None, bound.committed) else []),
        ]
        for bound in bounds.values()
    ]
    return [dict(zip(bounds, choice, strict=True)) for choice in product(*options)]


def temp_relation(layer: str, name: str) -> str:
    return f"{TEMP_PREFIX}_{layer}_{name}"


def compare(lake: DataFrame, postgres: DataFrame, sample_size: int = SAMPLE_SIZE) -> Matching:
    """The rows only the lake view or only the PostgreSQL view has, as multisets."""
    postgres = aligned(postgres, lake.schema)
    lake_only, postgres_only = differences(lake, postgres)
    return Matching(
        lake_rows=lake.count(),
        postgres_rows=postgres.count(),
        lake_only=lake_only.count(),
        postgres_only=postgres_only.count(),
        samples={
            "lake_only": sample(lake_only, lake.columns, sample_size),
            "postgres_only": sample(postgres_only, lake.columns, sample_size),
        },
    )


class GoldCheck:
    def __init__(
        self,
        spark: SparkSession,
        settings: GoldSettings,
        readers: GoldReaders,
        views: GoldLakeViews | None = None,
    ) -> None:
        self.spark = spark
        self.settings = settings
        self.readers = readers
        self.lake = views or GoldLakeViews(settings.lake)
        self.computed = GoldLakeViews(
            settings.lake,
            self.lake.tables,
            self.lake.dimensions,
            self.lake.facts,
            relation=temp_relation,
            version_keys=True,
        )

    @property
    def loads(self) -> tuple[Scd2Dimension | ChangeFact, ...]:
        return (*self.lake.dimensions, *self.lake.facts)

    def compared(self) -> list[str]:
        """The relations compared, by name."""
        names = [view.rpartition(".")[2] for view in self.lake.definitions()]
        return [name for name in names if name != FRESHNESS]

    def _bounds(self, read: BatchReader) -> dict[str, int] | None:
        snapshots: dict[str, int] = {}
        for load in self.loads:
            checkpoint = self.settings.checkpoint_path(load.name)
            batch = read(checkpoint)
            if batch is None:
                return None
            snapshots[load.entity.name] = snapshot_id(checkpoint, batch)
        return snapshots

    def _planned(self) -> dict[str, int]:
        snapshots: dict[str, int] = {}
        for load in self.loads:
            checkpoint = self.settings.checkpoint_path(load.name)
            batch = self.readers.planned(checkpoint)
            if batch is not None:
                snapshots[load.entity.name] = snapshot_id(checkpoint, batch)
        return snapshots

    def history_starts(self) -> dict[str, datetime | None]:
        """The first day of the oldest month each history fact still holds in PostgreSQL, by
        entity."""
        starts: dict[str, datetime | None] = {}
        for fact in self.lake.facts:
            changed_at = FactColumn.CHANGED_AT
            query = (
                f"(SELECT date_trunc('month', min({changed_at})) AS start"
                f" FROM {INTERNAL_SCHEMA}.{fact.history.name}) AS history_start"
            )
            (row,) = self.readers.read_postgres(self.spark, query).collect()
            starts[fact.entity.name] = row["start"]
        return starts

    def _postgres(self, name: str, starts: Mapping[str, datetime | None]) -> DataFrame:
        """PostgreSQL's public view, with each surrogate key replaced by the `_commit_sequence`
        of the dimension version it names."""
        frame = self.readers.read_postgres(self.spark, f"{PUBLIC_SCHEMA}.{name}")
        for dimension in self.lake.dimensions:
            key = dimension.surrogate_key
            if key not in frame.columns:
                continue
            versions = self.readers.read_postgres(
                self.spark, f"{INTERNAL_SCHEMA}.{dimension.name}"
            ).select(
                F.col(key).alias("_version_key"),
                F.col(Scd2Column.COMMIT_SEQUENCE).alias("_version_sequence"),
            )
            frame = (
                frame.join(versions, frame[key].eqNullSafe(versions["_version_key"]), "left")
                .withColumn(key, F.col("_version_sequence"))
                .drop("_version_key", "_version_sequence")
            )
        start = self._history_start(name, starts)
        if start is not None:
            frame = frame.where(F.col(FactColumn.CHANGED_AT) >= F.lit(start))
        columns = self.computed.definitions()[self.computed.view(name)].columns
        return frame.select(*columns).localCheckpoint(eager=True)

    def _history_start(self, name: str, starts: Mapping[str, datetime | None]) -> datetime | None:
        facts = {fact.history.name: fact.entity.name for fact in self.lake.facts}
        return starts.get(facts[name]) if name in facts else None

    def _register_silver(self, snapshots: Snapshots, starts: Mapping[str, datetime | None]) -> None:
        fact_entities = {fact.entity.name for fact in self.lake.facts}
        for load in self.loads:
            entity = load.entity
            table = self.settings.lake.table(SilverSettings.LAYER, entity.name)
            rows = self.readers.read_snapshot(self.spark, table, snapshots[entity.name])
            name = temp_relation(SilverSettings.LAYER, entity.name)
            rows.createOrReplaceTempView(name)
            start = starts.get(entity.name) if entity.name in fact_entities else None
            source = name
            if start is not None:
                source = f"{name}_window"
                rows.where(F.col(FactColumn.CHANGED_AT) >= F.lit(start)).createOrReplaceTempView(
                    source
                )
            history = TABLE_TEMPLATES[HISTORY].table_name(entity)
            query = history_query(Entity(entity), source)
            self.spark.sql(query).createOrReplaceTempView(
                temp_relation(SilverSettings.LAYER, history)
            )

    def lake_rows(
        self, snapshots: Snapshots, starts: Mapping[str, datetime | None]
    ) -> dict[str, DataFrame]:
        """The views' rows at the snapshots, by name, from their SQL over silver."""
        self._register_silver(snapshots, starts)
        rows: dict[str, DataFrame] = {}
        for view, definition in self.computed.definitions().items():
            frame = self.spark.sql(definition.query).toDF(*definition.columns)
            frame.createOrReplaceTempView(view)
            rows[view.removeprefix(f"{TEMP_PREFIX}_{GoldSettings.LAYER}_")] = frame
        return rows

    def _stale(self, name: str, definition: ViewDefinition) -> bool:
        stored = self.readers.read_stored_view(self.spark, self.lake.view(name))
        return stored != StoredView.of(definition)

    def run(self, sample_size: int = SAMPLE_SIZE) -> list[CheckResult]:
        names = self.compared()
        committed = self._bounds(self.readers.committed)
        if committed is None:
            metrics = dict.fromkeys(METRICS)
            return [
                NOT_CHECKED.result(self.lake.view(name), passed=False, metrics=metrics)
                for name in names
            ]
        starts = self.history_starts()
        postgres = {name: self._postgres(name, starts) for name in names}
        planned = self._planned()
        bounds = {
            entity: Bounds(snapshot, planned.get(entity)) for entity, snapshot in committed.items()
        }
        definitions = self.lake.definitions()
        stale = {name: self._stale(name, definitions[self.lake.view(name)]) for name in names}
        best: dict[str, tuple[Matching, Snapshots]] = {}
        for snapshots in snapshot_choices(bounds):
            lake = self.lake_rows(snapshots, starts)
            for name in names:
                if name in best and best[name][0].passed:
                    continue
                found = compare(lake[name], postgres[name], sample_size)
                if name not in best or found.passed:
                    best[name] = (found, snapshots)
            if all(found.passed for found, _ in best.values()):
                break
        return [self._result(name, *best[name], stale=stale[name], starts=starts) for name in names]

    def _result(
        self,
        name: str,
        found: Matching,
        snapshots: Snapshots,
        *,
        stale: bool,
        starts: Mapping[str, datetime | None],
    ) -> CheckResult:
        metrics = {
            "lake_rows": found.lake_rows,
            "postgres_rows": found.postgres_rows,
            "lake_only": found.lake_only,
            "postgres_only": found.postgres_only,
            "stale": int(stale),
        }
        details: dict[str, object] = {"snapshots": dict(snapshots), **found.samples}
        start = self._history_start(name, starts)
        if start is not None:
            details["history_from"] = start.isoformat()
        return GOLD_MATCHES_SILVER.result(
            self.lake.view(name),
            passed=found.passed and not stale,
            metrics=metrics,
            details=details,
        )


def check_gold(
    spark: SparkSession,
    settings: GoldSettings,
    readers: GoldReaders,
    views: GoldLakeViews | None = None,
) -> list[CheckResult]:
    return GoldCheck(spark, settings, readers, views).run()


def postgres_reader(settings: AnalyticsDbSettings) -> PostgresReader:
    """Reads a PostgreSQL relation, or a subquery with an alias, through Spark's JDBC source."""

    def read(spark: SparkSession, relation: str) -> DataFrame:
        return spark.read.format("jdbc").options(**jdbc_options(settings, relation)).load()

    return read


def gold_readers(
    spark: SparkSession, settings: GoldSettings, files: CheckpointFiles | None = None
) -> GoldReaders:
    files = files or HadoopCheckpointFiles(spark)
    return GoldReaders(
        committed=lambda checkpoint: committed_batch(files, checkpoint),
        planned=lambda checkpoint: planned_batch(files, checkpoint),
        read_snapshot=read_snapshot,
        read_postgres=postgres_reader(settings.analytics_db),
    )


def run_checks(spark: SparkSession, settings: GoldSettings) -> list[CheckResult]:
    return check_gold(spark, settings, gold_readers(spark, settings))
