from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from pipelines.lake.gold.facts import ChangeFact, FactColumn
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, Scd2Dimension
from pipelines.schemas.gold_definitions import GoldTable

COMMIT_SEQUENCE = str(FactColumn.COMMIT_SEQUENCE)
STAGING_PREFIX = "stage_"
STAGING_DESCRIPTION = (
    "Rows of the micro-batch being copied into {table}, loaded by Spark and replaced by every"
    " batch; unlogged, so it may be empty after a crash"
)
SEEDED = ("dim_order_status", "dim_date")
SEED_KEYS: Mapping[str, str] = {"dim_order_status": "status_key", "dim_date": "date_key"}


class WriteMode(StrEnum):
    """How the rows a batch computes for a table are written to it."""

    MERGE = "merge"
    """Matched on the keys: every column updated; new rows inserted."""
    MERGE_NEWER = "merge_newer"
    """As MERGE, but a matched row is updated only by a higher `_commit_sequence`."""


@dataclass(frozen=True, slots=True)
class StoredTable:
    """A gold table the gold job writes to Iceberg and copies into gold_internal in PostgreSQL.
    Its staging table takes nulls in every column.

    `history` tables are partitioned by month of `_changed_at` in PostgreSQL, where retention
    drops old months, and bucketed by their first key in Iceberg, which keeps every change.
    `unique` is a column PostgreSQL keeps unique besides the key, such as a hashed surrogate
    key, so a hash collision is refused instead of mixing two rows.
    """

    table: GoldTable
    mode: WriteMode
    keys: tuple[str, ...]
    history: bool = False
    unique: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.table.columns)

    @property
    def relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.name}"

    @property
    def postgres_key(self) -> tuple[str, ...]:
        """The PostgreSQL primary key; a history table's holds `_changed_at` because PostgreSQL
        requires the partition column in it."""
        if self.history:
            return (*self.keys, str(FactColumn.CHANGED_AT))
        return self.keys

    @property
    def staging_relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.staging_table.name}"

    @property
    def staging_table(self) -> GoldTable:
        return GoldTable(
            name=f"{STAGING_PREFIX}{self.name}",
            description=STAGING_DESCRIPTION.format(table=self.relation),
            columns=tuple(replace(column, not_null=False) for column in self.table.columns),
        )


def stored_tables(
    tables: Mapping[str, GoldTable],
    dimensions: Iterable[Scd2Dimension],
    facts: Iterable[ChangeFact],
) -> Mapping[str, StoredTable]:
    """Every stored gold table by name: the seeded and SCD2 dimensions, then each fact's history
    and current state."""
    stored = [StoredTable(tables[name], WriteMode.MERGE, (SEED_KEYS[name],)) for name in SEEDED]
    stored += [
        StoredTable(
            dimension.table,
            WriteMode.MERGE,
            (dimension.key, COMMIT_SEQUENCE),
            unique=(dimension.surrogate_key,),
        )
        for dimension in dimensions
    ]
    facts = tuple(facts)
    stored += [
        StoredTable(fact.history, WriteMode.MERGE, (fact.key, COMMIT_SEQUENCE), history=True)
        for fact in facts
    ]
    stored += [StoredTable(fact.current, WriteMode.MERGE_NEWER, (fact.key,)) for fact in facts]
    return {table.name: table for table in stored}
