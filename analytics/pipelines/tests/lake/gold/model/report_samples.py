from collections.abc import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    Values,
    seeded,
    staged,
    stored,
)
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.model.facts import current_rows, merged_current
from pipelines.lake.gold.model.reports import IS_REMOVED
from pipelines.lake.gold.model.versions import all_of


def merged_report(
    report: DataFrame, keys: Sequence[str], table: DataFrame | None = None
) -> DataFrame:
    """A stored report table after merging a recount: removed keys deleted, the others
    replaced, matching keys null-safely."""
    rows = report.where(~F.col(IS_REMOVED)).drop(IS_REMOVED)
    if table is None:
        return rows
    renamed = report.select(*(F.col(key).alias(f"__report_{key}") for key in keys))
    matching = all_of([F.col(key).eqNullSafe(F.col(f"__report_{key}")) for key in keys])
    return table.join(renamed, matching, "left_anti").unionByName(rows)


class CurrentState:
    """Current-state tables held in memory, merged batch by batch as the writer would, keeping
    the rows of the last batch's keys as they were before and after the merge."""

    def __init__(self, spark: SparkSession, fact: ChangeFact, rows: Iterable[Values] = ()) -> None:
        self.spark = spark
        self.fact = fact
        self.rows = stored(spark, fact.current, rows, versioned=False)
        self.before = self.rows.limit(0)
        self.after = self.rows.limit(0)

    def apply(self, batch: Iterable[Values]) -> DataFrame:
        """Merge a batch; returns the changed rows, before and after the merge."""
        key = self.fact.key
        changes = staged(self.spark, self.fact, batch)
        upserts = current_rows(self.fact, changes, self.rows, seeded(self.spark))
        keys = changes.select(key).distinct()
        self.before = self.rows.join(keys, key, "left_semi")
        self.rows = merged_current(self.rows, upserts, key).localCheckpoint()
        self.after = self.rows.join(keys, key, "left_semi")
        return self.before.unionByName(self.after)
