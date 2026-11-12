from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.latency.probe import Change, ProbeSetupError, ProbeTarget
from pipelines.latency.sql import Row, identifier, relation, single_integer


@dataclass(frozen=True, slots=True)
class GoldTable:
    relation: str
    key: str


def gold_tables(
    loads: Iterable[Scd2Dimension | ChangeFact] = (*DIMENSIONS, *FACTS),
) -> dict[str, GoldTable]:
    """The gold_internal table each entity's changes are appended to, by entity."""
    return {load.entity.name: GoldTable(load.relation, load.key) for load in loads}


def gold_lookup_query(table: GoldTable, target: ProbeTarget, change: Change) -> str:
    return (
        f"SELECT count(*) FROM {relation(*table.relation.split('.'))}"
        f" WHERE {identifier(table.key)} = {target.key}"
        f" AND {identifier(MetadataColumn.COMMIT_SEQUENCE)} = {change.commit_sequence}"
    )


@dataclass(frozen=True, slots=True)
class GoldLookup:
    """Whether PostgreSQL gold holds a change: gold appends every change, folded or not, to its
    gold_internal tables, so a change is visible there once the gold job has written it."""

    tables: Mapping[str, GoldTable]
    query: Callable[[str], Sequence[Row]]

    def visible(self, target: ProbeTarget, change: Change) -> bool:
        table = self.tables.get(target.name)
        if table is None:
            raise ProbeSetupError(f"no gold table is loaded from {target.name}")
        return single_integer(self.query(gold_lookup_query(table, target, change))) > 0
