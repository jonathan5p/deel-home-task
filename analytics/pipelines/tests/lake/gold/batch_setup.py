from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    frame,
    merged,
    schema,
    seeded,
    stored,
)
from pipelines.lake.gold.batch import GoldBatches, Write
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED

VERSIONED = {
    *(dimension.name for dimension in DIMENSIONS),
    *(fact.history.name for fact in FACTS),
}


def batches(spark: SparkSession) -> GoldBatches:
    return GoldBatches(STORED, DIMENSIONS, FACTS, seeded(spark))


class MemoryGold:
    """The stored gold tables in memory, merged write by write as the Iceberg tables are, with
    the names of the tables each batch read."""

    def __init__(self, spark: SparkSession) -> None:
        self.tables: dict[str, DataFrame] = dict(seeded(spark))
        self.reads: list[str] = []
        for name, table in STORED.items():
            if name in VERSIONED:
                self.tables[name] = stored(spark, table.table)
            elif name not in self.tables:
                self.tables[name] = frame(spark, schema(table.table.columns), [])

    def read(self, name: str) -> DataFrame:
        self.reads.append(name)
        return self.tables[name]

    def apply(self, writes: Iterable[Write]) -> list[Write]:
        applied = [Write(write.table, write.rows.localCheckpoint()) for write in writes]
        for write in applied:
            table = write.table
            after = merged(self.tables[table.name], write.rows, table.keys)
            self.tables[table.name] = after.localCheckpoint()
        return applied
