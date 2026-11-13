from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame

from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.model.dimensions import dimension_changes
from pipelines.lake.gold.model.facts import current_rows, history_changes, looked_up
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.stored import StoredTable

type Reader = Callable[[str], DataFrame]


@dataclass(frozen=True, slots=True)
class Write:
    """Rows a batch computed for a stored gold table, merged into it as its mode says."""

    table: StoredTable
    rows: DataFrame


class GoldBatches:
    """Computes the writes of one micro-batch of a gold load from its staged rows and the load's
    own stored tables, which `read` returns by name.

    A dimension batch writes the versions it adds or re-places. A fact batch writes its history
    and its current state, with the status and date keys looked up in the `seeded` dimensions.
    No load reads or writes another load's tables, so the loads' streams never wait for each
    other: a fact keeps its business keys, and the public views join the dimension version
    valid at each change when they are read.
    """

    def __init__(
        self,
        tables: Mapping[str, StoredTable],
        dimensions: Iterable[Scd2Dimension],
        facts: Iterable[ChangeFact],
        seeded: Mapping[str, DataFrame],
    ) -> None:
        self.tables = tables
        self.dimensions = {dimension.name: dimension for dimension in dimensions}
        self.facts = {fact.name: fact for fact in facts}
        self.seeded = seeded

    def tables_of(self, load: str) -> tuple[str, ...]:
        """The stored tables a load writes, and only it."""
        if load in self.dimensions:
            return (load,)
        fact = self.facts[load]
        return (fact.history.name, fact.current.name)

    def staged(self, load: str, batch: DataFrame) -> DataFrame:
        """The batch as the load's writes read it: a fact's with its looked-up keys."""
        if load in self.dimensions:
            return batch
        return looked_up(self.facts[load], batch, self.seeded)

    def writes(self, load: str, staged: DataFrame, read: Reader) -> list[Write]:
        """The rows to merge into each of the load's tables, from `staged` as `staged` returns
        it. Every write depends only on the stored rows and the batch, so a replay is
        idempotent."""
        if load in self.dimensions:
            return [self._dimension_write(self.dimensions[load], staged, read)]
        return self._fact_writes(self.facts[load], staged, read)

    def _write(self, name: str, rows: DataFrame) -> Write:
        return Write(self.tables[name], rows)

    def _dimension_write(self, dimension: Scd2Dimension, batch: DataFrame, read: Reader) -> Write:
        changes = dimension_changes(dimension, batch, read(dimension.name))
        return self._write(dimension.name, changes)

    def _fact_writes(self, fact: ChangeFact, batch: DataFrame, read: Reader) -> list[Write]:
        history = history_changes(fact, batch, read(fact.history.name))
        current = current_rows(fact, batch, read(fact.current.name))
        return [self._write(fact.history.name, history), self._write(fact.current.name, current)]
