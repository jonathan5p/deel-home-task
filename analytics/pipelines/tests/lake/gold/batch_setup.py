from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import frame, schema, seeded, stored
from pipelines.lake.gold.batch import Write, merged, recounted
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED
from pipelines.lake.gold.stored import WriteMode

VERSIONED = {
    *(dimension.name for dimension in DIMENSIONS),
    *(fact.history.name for fact in FACTS),
}


class MemoryGold:
    """The stored gold tables in memory, merged write by write as the Iceberg tables are."""

    def __init__(self, spark: SparkSession) -> None:
        self.tables: dict[str, DataFrame] = dict(seeded(spark))
        for name, table in STORED.items():
            if name in VERSIONED:
                self.tables[name] = stored(spark, table.table)
            elif name not in self.tables:
                self.tables[name] = frame(spark, schema(table.table.columns), [])

    def read(self, name: str) -> DataFrame:
        return self.tables[name]

    def apply(self, writes: Iterable[Write]) -> list[Write]:
        applied = [Write(write.table, write.rows.localCheckpoint()) for write in writes]
        for write in applied:
            table, rows = write.table, write.rows
            stored_rows = self.tables[table.name]
            if table.mode is WriteMode.REPLACE:
                self.tables[table.name] = rows.select(*stored_rows.columns)
            elif table.mode is WriteMode.RECOUNT:
                self.tables[table.name] = recounted(stored_rows, rows, table.keys)
            else:
                self.tables[table.name] = merged(stored_rows, rows, table.keys)
            self.tables[table.name] = self.tables[table.name].localCheckpoint()
        return applied
