from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import cast

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.batch_setup import MemoryGold, batches
from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    ITEMS,
    ORDERS,
    PRODUCTS,
    customer,
    item,
    order,
    product,
    staged,
)
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.public import PUBLIC_SCHEMA, PUBLIC_VIEWS, view_query
from pipelines.lake.gold.quality import REPORTS, GoldCheck, GoldReaders
from pipelines.lake.gold.serving import SYNC_STATE
from pipelines.quality.checks import CheckResult
from pipelines.schemas.gold_definitions import load_gold_tables
from pipelines.settings import GoldSettings

SNAPSHOT = 7
TODAY = date(2026, 11, 13)
SYNC_SCHEMA = "table_name string, snapshot_id long"
SETTINGS = GoldSettings.from_env(
    {
        "LAKE_ROOT": "s3a://lake",
        "ICEBERG_CATALOG": "lake",
        "ANALYTICS_DB_URL": "jdbc:postgresql://database:5432/analytics",
        "ANALYTICS_DB_USER": "user",
        "ANALYTICS_DB_PASSWORD": "password",
    }
)
GLOBAL_TEMP = "global_temp"

TABLES = load_gold_tables()

type Synced = dict[str, int | None]
type GoldTables = Mapping[str, DataFrame]


def consistent_gold(spark: SparkSession) -> dict[str, DataFrame]:
    """Every stored gold table as the gold job writes them for two customers, one with two
    versions, a product, three orders, one of them closed, and two items."""
    gold = MemoryGold(spark)
    batch_of = batches(spark)
    loads = (
        (CUSTOMERS, [customer(1, 10, "Ada"), customer(1, 20, "Bo"), customer(2, 15, "Cy")]),
        (PRODUCTS, [product(1, 11, "1.50")]),
        (
            ORDERS,
            [
                order(70, 30),
                order(70, 40, "PROCESSING"),
                order(71, 31, customer_id=2),
                order(72, 32, "COMPLETED"),
            ],
        ),
        (ITEMS, [item(80, 33, order_id=70), item(81, 34, order_id=71, quantity=5)]),
    )
    for load, rows in loads:
        batch = batch_of.staged(load.name, staged(spark, load, rows))
        gold.apply(batch_of.writes(load.name, batch, gold.read))
    return {name: rows.localCheckpoint() for name, rows in gold.tables.items()}


def answers(spark: SparkSession, tables: GoldTables) -> dict[str, DataFrame]:
    """PostgreSQL's answer to each business-question view over `tables`, by public view: the
    view's query run over the tables as global temporary views."""
    for name, rows in tables.items():
        rows.createOrReplaceGlobalTempView(name)
    views = {view.name: view for view in PUBLIC_VIEWS}
    return {
        f"{PUBLIC_SCHEMA}.{name}": spark.sql(
            view_query(views[name], TABLES, GLOBAL_TEMP)
        ).localCheckpoint()
        for name in REPORTS
    }


@dataclass
class FakeGold:
    """The gold tables in Iceberg, one snapshot of each, and their PostgreSQL copy, with the
    sync_state rows each read of it returns in turn, the last one from then on."""

    spark: SparkSession
    lake: dict[str, dict[int, DataFrame]]
    postgres: dict[str, DataFrame]
    sync_states: list[Synced]
    sync_reads: int = 0
    reads: list[str] = field(default_factory=list)

    @classmethod
    def mirrored(cls, spark: SparkSession, tables: GoldTables) -> FakeGold:
        copies = {STORED[name].relation: rows for name, rows in tables.items()}
        return cls(
            spark=spark,
            lake={name: {SNAPSHOT: rows} for name, rows in tables.items()},
            postgres=copies | answers(spark, tables),
            sync_states=[dict.fromkeys(tables, SNAPSHOT)],
        )

    def read(self, name: str) -> DataFrame:
        return next(iter(self.lake[name].values()))

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None:
        return self.lake[name].get(snapshot_id)

    def read_postgres(self, spark: SparkSession, relation: str) -> DataFrame:
        if relation != SYNC_STATE:
            self.reads.append(relation)
            return self.postgres[relation]
        state = self.sync_states[min(self.sync_reads, len(self.sync_states) - 1)]
        self.sync_reads += 1
        return spark.createDataFrame(list(state.items()), SYNC_SCHEMA)

    def change(self, name: str, edit: Callable[[DataFrame], DataFrame]) -> None:
        """Edit a table on both sides, so the copy still matches."""
        self.lake[name] = {snapshot: edit(rows) for snapshot, rows in self.lake[name].items()}
        relation = STORED[name].relation
        self.postgres[relation] = edit(self.postgres[relation])

    def check(self) -> GoldCheck:
        readers = GoldReaders(read_postgres=self.read_postgres, lake=self)
        return GoldCheck(self.spark, SETTINGS, readers, today=lambda: TODAY)

    def results(self) -> list[CheckResult]:
        return self.check().run()


def by_check(results: Sequence[CheckResult], check: str) -> dict[str, CheckResult]:
    """The results of one check by the name of the table they checked."""
    return {
        result.subject.rpartition(".")[2]: result for result in results if result.check == check
    }


def samples(result: CheckResult, side: str) -> tuple[Mapping[str, object], ...]:
    return cast("tuple[Mapping[str, object], ...]", result.details[side])
