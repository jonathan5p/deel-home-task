from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import cast

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.gold.lake.views_setup import ENTITIES, customer, item, order, product
from analytics.pipelines.tests.gold.silver_setup import silver_schema
from pipelines.gold.lake.views import GoldLakeViews
from pipelines.gold.quality import GoldCheck, GoldReaders
from pipelines.lake.checkpoints import CommittedBatch
from pipelines.lake.views import StoredView
from pipelines.quality.checks import CheckResult
from pipelines.settings import GoldSettings, SilverSettings

FAKE_KEY_OFFSET = 1000
SETTINGS = GoldSettings.from_env(
    {
        "LAKE_ROOT": "s3a://lake",
        "ICEBERG_CATALOG": "lake",
        "ANALYTICS_DB_URL": "jdbc:postgresql://database:5432/analytics",
        "ANALYTICS_DB_USER": "user",
        "ANALYTICS_DB_PASSWORD": "password",
    }
)
type SilverRows = Mapping[str, Sequence[Row]]


def base_silver() -> dict[str, list[Row]]:
    """A customer with two versions, a product, an order with two changes and one item."""
    return {
        "customers": [customer(1, 10, "Ana", op="create"), customer(1, 20, "Bo")],
        "products": [product(9, 5, "1.50", op="create")],
        "orders": [order(701, 30, "PENDING", op="create"), order(701, 40, "PROCESSING")],
        "order_items": [item(801, 31, 9, 2, order_id=701)],
    }


def batch(snapshot: int) -> CommittedBatch:
    return CommittedBatch(query_id="query", batch=1, sources=({"snapshot_id": snapshot},))


def batches(snapshots: Mapping[str, int]) -> dict[str, CommittedBatch]:
    """A batch per gold stream checkpoint, reading the entity's silver snapshot."""
    views = GoldLakeViews(SETTINGS.lake)
    loads = (*views.dimensions, *views.facts)
    return {
        SETTINGS.checkpoint_path(load.name): batch(snapshots[name])
        for load in loads
        if (name := load.entity.name) in snapshots
    }


@dataclass
class FakeLake:
    spark: SparkSession
    snapshots: dict[tuple[str, int], DataFrame] = field(default_factory=dict)
    committed: dict[str, CommittedBatch] = field(default_factory=dict)
    planned: dict[str, CommittedBatch] = field(default_factory=dict)
    postgres: dict[str, DataFrame] = field(default_factory=dict)
    history_starts: dict[str, datetime | None] = field(default_factory=dict)
    stale: set[str] = field(default_factory=set)

    def add_snapshot(self, snapshot: int, silver: SilverRows) -> None:
        for name, entity in ENTITIES.items():
            table = SETTINGS.lake.table(SilverSettings.LAYER, name)
            rows = list(silver.get(name, ()))
            self.snapshots[table, snapshot] = self.spark.createDataFrame(
                rows, silver_schema(entity)
            )

    def read_snapshot(self, spark: SparkSession, table: str, snapshot: int) -> DataFrame:
        return self.snapshots[table, snapshot]

    def read_postgres(self, spark: SparkSession, relation: str) -> DataFrame:
        if "history_start" in relation:
            (fact,) = [name for name in self.history_starts if f".{name})" in relation]
            return spark.createDataFrame([(self.history_starts[fact],)], "start timestamp")
        return self.postgres[relation]

    def read_stored_view(self, spark: SparkSession, view: str) -> StoredView | None:
        if view in self.stale:
            return None
        definition = GoldLakeViews(SETTINGS.lake).definitions()[view]
        return StoredView.of(definition)

    def readers(self) -> GoldReaders:
        return GoldReaders(
            committed=self.committed.get,
            planned=self.planned.get,
            read_snapshot=self.read_snapshot,
            read_postgres=self.read_postgres,
            read_stored_view=self.read_stored_view,
        )

    def check(self) -> GoldCheck:
        return GoldCheck(self.spark, SETTINGS, self.readers())

    def mirror_postgres(
        self, snapshots: Mapping[str, int], starts: Mapping[str, datetime | None] | None = None
    ) -> None:
        starts = starts or {}
        self.history_starts = {
            fact.history.name: starts.get(fact.entity.name)
            for fact in GoldLakeViews(SETTINGS.lake).facts
        }
        check = self.check()
        rows = check.lake_rows(snapshots, starts)
        keys = check.lake.surrogate_keys
        for name, frame in rows.items():
            self.postgres[f"gold.{name}"] = frame.select(
                *(
                    (F.col(c) + FAKE_KEY_OFFSET).alias(c) if c in keys else F.col(c)
                    for c in frame.columns
                )
            )
        for dimension in check.lake.dimensions:
            versions = rows[dimension.name].select(
                (F.col(dimension.surrogate_key) + FAKE_KEY_OFFSET).alias(dimension.surrogate_key),
                F.col(dimension.surrogate_key).alias("_commit_sequence"),
            )
            self.postgres[f"gold_internal.{dimension.name}"] = versions


def by_view(results: Sequence[CheckResult]) -> dict[str, CheckResult]:
    """The results by the name of the view they compared."""
    return {result.subject.rpartition(".")[2]: result for result in results}


def samples(result: CheckResult, side: str) -> tuple[Mapping[str, object], ...]:
    return cast("tuple[Mapping[str, object], ...]", result.details[side])
