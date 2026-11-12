from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NoReturn

from pipelines.lake.gold.model.columns import VERSION_COLUMNS
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, Scd2Dimension, silver_column
from pipelines.schemas.definitions import ColumnDefinition, EntityDefinition
from pipelines.schemas.gold_definitions import GoldTable


class FactColumn(StrEnum):
    COMMIT_SEQUENCE = "_commit_sequence"
    CHANGED_AT = "_changed_at"
    CONTENT_HASH = "_content_hash"
    IS_DELETED = "_is_deleted"


HISTORY_COLUMNS = (*FactColumn, *VERSION_COLUMNS)
CURRENT_COLUMNS = (FactColumn.COMMIT_SEQUENCE, FactColumn.CHANGED_AT, FactColumn.IS_DELETED)


class FactDefinitionError(ValueError):
    def __init__(self, table: str, problem: str) -> None:
        super().__init__(f"fact {table}: {problem}")


def _names(table: GoldTable) -> set[str]:
    return {column.name for column in table.columns}


@dataclass(frozen=True, slots=True)
class KeyLookup:
    """A gold column holding the `key` column (its key, or another attribute) of the row of a
    seeded dimension whose `code` equals a staged column; the value of the `fallback` code's
    row when no row matches, or null without one."""

    column: str
    source: str
    dimension: GoldTable
    code: str
    key: str
    fallback: str | None = None


@dataclass(frozen=True, slots=True)
class Multiplied:
    """A fact column computed as a staged column times a column an as-of lookup fills in."""

    column: str
    factor: str
    looked_up: str


@dataclass(frozen=True, slots=True)
class AsOfLookup:
    """History fact columns taken from the version of an SCD2 dimension valid when the change
    was committed: not folded, not deleted, and with the change's `_changed_at` in
    [`_valid_from`, `_valid_to`). A change older than the key's first version takes that
    version, because the source snapshots each table at a slightly different time, so an
    order or item can be snapshotted just before its customer or product. They are null when
    the key has no version yet, or the version at that time is a delete.

    `business_key` is the fact column holding the dimension's key, and `columns` pairs each
    fact column with the dimension column it takes.
    """

    dimension: Scd2Dimension
    business_key: str
    columns: tuple[tuple[str, str], ...]
    multiplied: tuple[Multiplied, ...] = ()

    @property
    def fact_columns(self) -> tuple[str, ...]:
        return (*(fact for fact, _ in self.columns), *(m.column for m in self.multiplied))


@dataclass(frozen=True, slots=True)
class ChangeFact:
    """The history fact and the current-state table of one source entity, both written from
    its silver table: every change is stored in `history` with its version columns, and the
    latest change of each key replaces its `current` row if it is newer.

    A column is loaded from the silver column its `from` names (`<entity>.<lake name>`), a
    metadata column from the silver column of the same name; `lookups` and `as_of` fill the
    other columns. The staged columns also hold the silver columns the lookups read that
    neither table keeps, such as the order status behind `status_key`.
    """

    history: GoldTable
    current: GoldTable
    entity: EntityDefinition
    lookups: tuple[KeyLookup, ...] = ()
    as_of: tuple[AsOfLookup, ...] = ()

    def __post_init__(self) -> None:
        for table, required in ((self.history, HISTORY_COLUMNS), (self.current, CURRENT_COLUMNS)):
            missing = sorted(set(required) - _names(table))
            if missing:
                self._fail(f"{table.name} has no columns named {', '.join(missing)}")
        filled = {lookup.column for lookup in self.lookups}
        unknown = sorted(filled - _names(self.history) - _names(self.current))
        if unknown:
            self._fail(f"lookups fill columns neither table has: {', '.join(unknown)}")
        for lookup in self.lookups:
            self._check_key_lookup(lookup)
        for lookup in self.as_of:
            self._check_as_of(lookup)
        self._key()

    def _fail(self, problem: str) -> NoReturn:
        raise FactDefinitionError(self.history.name, problem)

    def _check_key_lookup(self, lookup: KeyLookup) -> None:
        missing = sorted({lookup.code, lookup.key} - _names(lookup.dimension))
        if missing:
            self._fail(f"{lookup.dimension.name} has no columns named {', '.join(missing)}")
        self._staged_name(lookup.source)

    def _check_as_of(self, lookup: AsOfLookup) -> None:
        outside = sorted(set(lookup.fact_columns) - _names(self.history))
        if outside:
            history = self.history.name
            self._fail(f"as-of lookups fill columns {history} does not have: {', '.join(outside)}")
        dimension = lookup.dimension.table
        missing = sorted({column for _, column in lookup.columns} - _names(dimension))
        if missing:
            self._fail(f"{dimension.name} has no columns named {', '.join(missing)}")
        looked_up = {fact for fact, _ in lookup.columns}
        unfilled = sorted({m.looked_up for m in lookup.multiplied} - looked_up)
        if unfilled:
            self._fail(f"a multiplied column needs a looked-up column: {', '.join(unfilled)}")
        for column in (lookup.business_key, *(m.factor for m in lookup.multiplied)):
            self._staged_name(column)

    @property
    def name(self) -> str:
        return self.history.name

    @property
    def relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.history.name}"

    @property
    def current_relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.current.name}"

    @property
    def derived(self) -> frozenset[str]:
        """The columns the gold job computes, which are never loaded from silver: the looked-up
        ones and the version columns."""
        looked_up = (column for lookup in self.as_of for column in lookup.fact_columns)
        lookups = (lookup.column for lookup in self.lookups)
        return frozenset({*lookups, *looked_up, *VERSION_COLUMNS})

    def _table_columns(self) -> list[tuple[GoldTable, ColumnDefinition]]:
        """The columns of both tables loaded from silver, each once, with the table naming it
        first: the history's, then the current-state table's."""
        seen: set[str] = set()
        columns: list[tuple[GoldTable, ColumnDefinition]] = []
        for table in (self.history, self.current):
            for column in table.columns:
                if column.name not in seen and column.name not in self.derived:
                    seen.add(column.name)
                    columns.append((table, column))
        return columns

    def _inputs(self) -> list[str]:
        lookups = (lookup.source for lookup in self.lookups)
        as_of = (
            column
            for lookup in self.as_of
            for column in (lookup.business_key, *(m.factor for m in lookup.multiplied))
        )
        return [*lookups, *as_of]

    def _extra_inputs(self) -> list[ColumnDefinition]:
        """The silver columns the lookups read that neither table keeps, under their lake name."""
        kept = {column.name for _, column in self._table_columns()}
        by_lake_name = {column.lake_name: column for column in self.entity.all_columns}
        extra: dict[str, ColumnDefinition] = {}
        for name in self._inputs():
            if name not in kept and name in by_lake_name:
                extra[name] = replace(by_lake_name[name], name=name, renamed_to=None)
        return list(extra.values())

    def _staged_name(self, name: str) -> None:
        if name not in {column.name for column in self.staged_columns}:
            self._fail(
                f"lookups read {name}, which neither table nor silver {self.entity.name} has"
            )

    @property
    def staged_columns(self) -> tuple[ColumnDefinition, ...]:
        return (*(column for _, column in self._table_columns()), *self._extra_inputs())

    @property
    def silver_columns(self) -> Mapping[str, str]:
        """The silver column each staged column is loaded from, by staged column."""
        return self._sources()

    def _sources(self) -> Mapping[str, str]:
        sources = {
            column.name: silver_column(table, self.entity, column.name)
            for table, column in self._table_columns()
        }
        unmapped = [column for column, source in sources.items() if source is None]
        if unmapped:
            self._fail(
                f"no silver column of {self.entity.name} for {', '.join(unmapped)};"
                " give it a from or a lookup"
            )
        extra = {column.name: column.name for column in self._extra_inputs()}
        return {column: source for column, source in sources.items() if source is not None} | extra

    @property
    def key(self) -> str:
        """The column loaded from the entity's key, which identifies a row's changes."""
        return self._key()

    def _key(self) -> str:
        (entity_key,) = (c for c in self.entity.columns if c.name == self.entity.key)
        lake_name = entity_key.lake_name
        keys = [gold for gold, silver in self._sources().items() if silver == lake_name]
        if len(keys) != 1 or keys[0] not in _names(self.current):
            self._fail(
                f"exactly one column of both tables must come from"
                f" {self.entity.name}.{entity_key.name}"
            )
        return keys[0]

    def check_silver(self, columns: Iterable[str]) -> None:
        """Fail if the silver input lacks a column the fact is loaded from."""
        missing = sorted(set(self.silver_columns.values()) - set(columns))
        if missing:
            self._fail(f"the silver input has no columns named {', '.join(missing)}")
