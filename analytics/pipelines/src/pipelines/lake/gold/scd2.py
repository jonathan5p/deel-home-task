from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import NoReturn

from pipelines.lake.gold.model.columns import VERSION_COLUMNS
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.schemas.definitions import ColumnDefinition, EntityDefinition
from pipelines.schemas.gold_definitions import GoldTable

INTERNAL_SCHEMA = "gold_internal"


class Scd2Column(StrEnum):
    COMMIT_SEQUENCE = "_commit_sequence"
    CONTENT_HASH = "_content_hash"
    IS_DELETED = "_is_deleted"
    VALID_FROM = "_valid_from"


SILVER_COLUMNS: Mapping[str, str] = {Scd2Column.VALID_FROM: str(MetadataColumn.CHANGED_AT)}


class Scd2DefinitionError(ValueError):
    def __init__(self, table: str, problem: str) -> None:
        super().__init__(f"SCD2 dimension {table}: {problem}")


def postgres_string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def silver_column(
    table: GoldTable,
    entity: EntityDefinition,
    column: str,
    renamed: Mapping[str, str] | None = None,
) -> str | None:
    """The silver column of `entity` a gold column is loaded from, or None if it has none: a
    rename, the column its `from` names (`<entity>.<lake name>`), or a metadata column of the
    same name (`_valid_from` comes from `_changed_at`)."""
    renamed = renamed or {}
    if column in renamed:
        return renamed[column]
    if column in SILVER_COLUMNS:
        return SILVER_COLUMNS[column]
    reference = table.reference(column)
    if reference is None:
        return column if column.startswith("_") else None
    owner, _, lake_name = reference.partition(".")
    return lake_name if owner == entity.name else None


@dataclass(frozen=True, slots=True)
class Scd2Dimension:
    """An SCD2 dimension loaded from the silver table of one source entity.

    Every column comes from the silver column its `from` names (`<entity>.<lake name>`), a
    metadata column from the silver column of the same name, and `_valid_from` from
    `_changed_at`; `renamed` gives the silver column of any other gold column, as
    (gold column, silver column) pairs. `surrogate_key` and the version columns are computed
    by the gold job.
    """

    table: GoldTable
    entity: EntityDefinition
    surrogate_key: str
    renamed: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        required = {self.surrogate_key, *Scd2Column, *VERSION_COLUMNS}
        missing = sorted(required - {column.name for column in self.table.columns})
        if missing:
            self._fail(f"the table has no columns named {', '.join(missing)}")
        self._key()

    def _fail(self, problem: str) -> NoReturn:
        raise Scd2DefinitionError(self.table.name, problem)

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.name}"

    @property
    def staged_columns(self) -> tuple[ColumnDefinition, ...]:
        """The columns loaded from silver: all but the surrogate key and the version columns."""
        computed = {self.surrogate_key, *VERSION_COLUMNS}
        return tuple(column for column in self.table.columns if column.name not in computed)

    @property
    def silver_columns(self) -> Mapping[str, str]:
        """The silver column each staged column is loaded from, by gold column."""
        return self._sources()

    def _sources(self) -> Mapping[str, str]:
        staged = [column.name for column in self.staged_columns]
        unknown = sorted(set(dict(self.renamed)) - set(staged))
        if unknown:
            self._fail(f"renamed names columns it does not stage: {', '.join(unknown)}")
        renamed = dict(self.renamed)
        sources = {
            column: silver_column(self.table, self.entity, column, renamed) for column in staged
        }
        unmapped = [column for column, source in sources.items() if source is None]
        if unmapped:
            self._fail(
                f"no silver column of {self.entity.name} for {', '.join(unmapped)};"
                " give it a from or a rename"
            )
        return {column: source for column, source in sources.items() if source is not None}

    @property
    def key(self) -> str:
        """The gold column loaded from the entity's key, which identifies a row's versions."""
        return self._key()

    def _key(self) -> str:
        (entity_key,) = (c for c in self.entity.columns if c.name == self.entity.key)
        lake_name = entity_key.lake_name
        keys = [gold for gold, silver in self._sources().items() if silver == lake_name]
        if len(keys) != 1:
            self._fail(f"exactly one column must come from {self.entity.name}.{entity_key.name}")
        return keys[0]

    def check_silver(self, columns: Iterable[str]) -> None:
        """Fail if the silver input lacks a column the dimension is loaded from."""
        missing = sorted(set(self.silver_columns.values()) - set(columns))
        if missing:
            self._fail(f"the silver input has no columns named {', '.join(missing)}")
