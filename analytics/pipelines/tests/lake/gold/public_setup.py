from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import Values, seeded, staged
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED
from pipelines.lake.gold.lake import IcebergGold
from pipelines.lake.gold.model.dimensions import dimension_changes
from pipelines.lake.gold.model.facts import current_rows, history_changes, looked_up
from pipelines.lake.gold.public import PUBLIC_VIEWS, view_query
from pipelines.schemas.gold_definitions import load_gold_tables

INTERNAL = "gold_internal"
TABLES = load_gold_tables()
VIEWS = {view.name: view for view in PUBLIC_VIEWS}
LOADS = {load.name: load for load in (*DIMENSIONS, *FACTS)}


class LakeGold:
    """The gold tables in a local Iceberg catalog, loaded batch by batch as the gold job does,
    and the public views queried as the lake renders them."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        self.spark = spark
        self.namespace = f"{catalog}.{INTERNAL}"
        relation = {name: f"{self.namespace}.{name}" for name in STORED}
        self.lake = IcebergGold(spark, STORED, relation)
        self.lake.ensure()
        for name, rows in seeded(spark).items():
            self.lake.write(STORED[name], rows)

    def load(self, name: str, changes: Iterable[Values]) -> None:
        load = LOADS[name]
        batch = staged(self.spark, load, list(changes))
        if name in {dimension.name for dimension in DIMENSIONS}:
            dimension = next(d for d in DIMENSIONS if d.name == name)
            rows = dimension_changes(dimension, batch, self.lake.read(name))
            self.lake.write(STORED[name], rows.localCheckpoint())
            return
        fact = next(f for f in FACTS if f.name == name)
        batch = looked_up(fact, batch, seeded(self.spark)).localCheckpoint()
        history = history_changes(fact, batch, self.lake.read(fact.history.name))
        current = current_rows(fact, batch, self.lake.read(fact.current.name))
        self.lake.write(STORED[fact.history.name], history.localCheckpoint())
        self.lake.write(STORED[fact.current.name], current.localCheckpoint())

    def view(self, name: str) -> DataFrame:
        return self.spark.sql(view_query(VIEWS[name], TABLES, self.namespace))
