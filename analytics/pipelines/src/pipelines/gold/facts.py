from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NoReturn

from pipelines.gold.scd2 import (
    INTERNAL_SCHEMA,
    STAGING_DESCRIPTION,
    STAGING_PREFIX,
    Scd2Column,
    Scd2Dimension,
    lines,
    postgres_string,
    silver_column,
)
from pipelines.schemas.definitions import ColumnDefinition, EntityDefinition
from pipelines.schemas.gold_definitions import GoldTable

ROW = "fact_row"


class FactColumn(StrEnum):
    COMMIT_SEQUENCE = "_commit_sequence"
    CHANGED_AT = "_changed_at"
    CONTENT_HASH = "_content_hash"
    IS_DELETED = "_is_deleted"


HISTORY_COLUMNS = tuple(FactColumn)
CURRENT_COLUMNS = (FactColumn.COMMIT_SEQUENCE, FactColumn.CHANGED_AT, FactColumn.IS_DELETED)


class FactDefinitionError(ValueError):
    def __init__(self, table: str, problem: str) -> None:
        super().__init__(f"fact {table}: {problem}")


def _names(table: GoldTable) -> set[str]:
    return {column.name for column in table.columns}


@dataclass(frozen=True, slots=True)
class KeyLookup:
    """A gold column holding the key of the row of a seeded dimension whose `code` equals a
    staged column; the key of the `fallback` code when no row matches, or null without one."""

    column: str
    source: str
    dimension: GoldTable
    code: str
    key: str
    fallback: str | None = None

    def expression(self, row: str) -> str:
        relation = f"{INTERNAL_SCHEMA}.{self.dimension.name}"

        def key_of(value: str) -> str:
            return (
                f"(SELECT lookup.{self.key} FROM {relation} AS lookup"
                f" WHERE lookup.{self.code} = {value})"
            )

        matched = key_of(f"{row}.{self.source}")
        if self.fallback is None:
            return matched
        return f"coalesce({matched}, {key_of(postgres_string(self.fallback))})"


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
    def alias(self) -> str:
        return self.dimension.name

    @property
    def fact_columns(self) -> tuple[str, ...]:
        return (*(fact for fact, _ in self.columns), *(m.column for m in self.multiplied))

    def expressions(self, row: str) -> Mapping[str, str]:
        looked_up = {fact: f"{self.alias}.{column}" for fact, column in self.columns}
        multiplied = {
            m.column: f"{row}.{m.factor} * {looked_up[m.looked_up]}" for m in self.multiplied
        }
        return looked_up | multiplied

    def join(self, row: str) -> str:
        """The lateral join that finds the version in the dimension's versions view, which
        derives the fold flag and validity. The first version by `_commit_sequence` is
        never folded, so it also covers every earlier time unless it is a delete. When clock skew
        makes two ranges overlap, the version committed last wins, so the lookup always yields
        at most one row."""
        dimension = self.dimension
        selected = ", ".join(f"version.{column}" for _, column in self.columns)
        valid_from = Scd2Column.VALID_FROM
        changed_at = f"{row}.{FactColumn.CHANGED_AT}"
        sequence = Scd2Column.COMMIT_SEQUENCE
        return f"""LEFT JOIN LATERAL (
    SELECT {selected}
    FROM {dimension.versions_relation} AS version
    WHERE version.{dimension.key} = {row}.{self.business_key}
      AND NOT version._is_folded
      AND NOT version.{Scd2Column.IS_DELETED}
      AND (
          version.{valid_from} <= {changed_at}
          OR NOT EXISTS (
              SELECT 1
              FROM {dimension.relation} AS earlier
              WHERE earlier.{dimension.key} = version.{dimension.key}
                AND earlier.{sequence} < version.{sequence}
          )
      )
      AND (version._valid_to IS NULL OR {changed_at} < version._valid_to)
    ORDER BY version.{sequence} DESC
    LIMIT 1
) AS {self.alias} ON true"""


@dataclass(frozen=True, slots=True)
class ChangeFact:
    """The history fact and the current-state table of one source entity, both loaded from its
    silver table in one transaction per micro-batch: every change is appended to `history`,
    and the latest change of each key replaces its `current` row if it is newer.

    A column is loaded from the silver column its `from` names (`<entity>.<lake name>`), a
    metadata column from the silver column of the same name; `lookups` and `as_of` fill the
    other columns. Staging also holds the silver columns the lookups read that neither table
    keeps, such as the order status behind `status_key`.
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
    def staging_relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.staging_table.name}"

    @property
    def derived(self) -> frozenset[str]:
        """The columns the merge fills in, which are never loaded from silver."""
        looked_up = (column for lookup in self.as_of for column in lookup.fact_columns)
        return frozenset({*(lookup.column for lookup in self.lookups), *looked_up})

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
    def staging_table(self) -> GoldTable:
        tables = f"{self.relation} and {self.current_relation}"
        return GoldTable(
            name=f"{STAGING_PREFIX}{self.name}",
            description=STAGING_DESCRIPTION.format(table=tables),
            columns=self.staged_columns,
        )

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

    @property
    def history_key(self) -> tuple[str, ...]:
        """The history's primary key; it holds `_changed_at` because PostgreSQL requires the
        partition column in it."""
        return (self.key, FactColumn.COMMIT_SEQUENCE, FactColumn.CHANGED_AT)

    def check_silver(self, columns: Iterable[str]) -> None:
        """Fail if the silver input lacks a column the fact is loaded from."""
        missing = sorted(set(self.silver_columns.values()) - set(columns))
        if missing:
            self._fail(f"the silver input has no columns named {', '.join(missing)}")

    def _expressions(self, table: GoldTable) -> tuple[Mapping[str, str], list[str]]:
        """The expression of each column of `table` over the staged row, and the joins they
        need."""
        expressions = {column.name: f"{ROW}.{column.name}" for column in self.staged_columns}
        expressions |= {lookup.column: lookup.expression(ROW) for lookup in self.lookups}
        joins: list[str] = []
        for lookup in self.as_of:
            if set(lookup.fact_columns) & _names(table):
                expressions |= lookup.expressions(ROW)
                joins.append(lookup.join(ROW))
        return expressions, joins

    def _select(self, table: GoldTable, source: str) -> str:
        expressions, joins = self._expressions(table)
        selected = lines(expressions[column.name] for column in table.columns)
        return "\n".join([f"SELECT\n{selected}", f"FROM {source} AS {ROW}", *joins])

    def history_statement(self) -> str:
        """Append the staged changes not stored yet; a replayed change is skipped."""
        targets = lines(column.name for column in self.history.columns)
        return (
            f"INSERT INTO {self.relation} (\n{targets}\n)\n"
            f"{self._select(self.history, self.staging_relation)}\n"
            f"ON CONFLICT ({', '.join(self.history_key)}) DO NOTHING"
        )

    def current_statement(self) -> str:
        """Apply the latest staged change of each key to the current-state table, only if it
        is newer than the stored one, so a late or replayed change never overwrites newer
        state. A delete is kept as a deleted row with its `_commit_sequence`, so an older
        change arriving later cannot bring the row back."""
        key, sequence = self.key, FactColumn.COMMIT_SEQUENCE
        latest = (
            f"(\n    SELECT DISTINCT ON ({key}) *\n    FROM {self.staging_relation}\n"
            f"    ORDER BY {key}, {sequence} DESC\n)"
        )
        targets = lines(column.name for column in self.current.columns)
        updates = lines(
            f"{column.name} = excluded.{column.name}"
            for column in self.current.columns
            if column.name != key
        )
        return (
            f"INSERT INTO {self.current_relation} AS stored (\n{targets}\n)\n"
            f"{self._select(self.current, latest)}\n"
            f"ON CONFLICT ({key}) DO UPDATE SET\n{updates}\n"
            f"WHERE excluded.{sequence} > stored.{sequence}"
        )

    def merge_statements(self) -> Sequence[str]:
        """The statements that merge the staged batch, to run in this order in one transaction."""
        return (self.history_statement(), self.current_statement())

    def _resolve_statement(self, lookup: AsOfLookup) -> str:
        dimension = lookup.dimension
        expressions = lookup.expressions(ROW)
        resolved = lines(
            [
                *(f"{ROW}.{column}" for column in self.history_key),
                *(f"{expressions[column]} AS {column}" for column in lookup.fact_columns),
            ],
            indent="        ",
        )
        assignments = lines(f"{column} = resolved.{column}" for column in lookup.fact_columns)
        stored = ", ".join(f"stored.{column}" for column in lookup.fact_columns)
        changed = ", ".join(f"resolved.{column}" for column in lookup.fact_columns)
        conditions = [
            *(f"stored.{column} = resolved.{column}" for column in self.history_key),
            f"({stored}) IS DISTINCT FROM ({changed})",
        ]
        where = "\n  AND ".join(conditions)
        join = "\n".join(f"    {line}" for line in lookup.join(ROW).splitlines())
        touched = f"SELECT {dimension.key} FROM {dimension.staging_relation}"
        return (
            f"WITH resolved AS (\n    SELECT\n{resolved}\n    FROM {self.relation} AS {ROW}\n"
            f"{join}\n"
            f"    WHERE {ROW}.{lookup.business_key} IN ({touched})\n)\n"
            f"UPDATE {self.relation} AS stored\n"
            f"SET\n{assignments}\n"
            "FROM resolved\n"
            f"WHERE {where}"
        )

    def resolve_statements(self, dimension: str) -> Sequence[str]:
        """Re-resolve the as-of lookups on `dimension` for the history rows of every key its
        staged batch touched, updating only the rows whose looked-up values change.

        Run after the dimension's merge in the same transaction: a late dimension version can
        split a validity range or unfold a version, and a dimension row can arrive after the
        facts that need it, so values resolved when a change was stored can go stale.
        """
        return tuple(
            self._resolve_statement(lookup)
            for lookup in self.as_of
            if lookup.dimension.name == dimension
        )
