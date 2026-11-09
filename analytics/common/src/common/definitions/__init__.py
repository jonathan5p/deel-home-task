import string
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from importlib.resources import files
from importlib.resources.abc import Traversable

DEFINITIONS: Traversable = files(__name__)

type Fields = Mapping[str, object]


class LogicalType(StrEnum):
    BIGINT = "bigint"
    INTEGER = "integer"
    VARCHAR = "varchar"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    DECIMAL = "decimal"


class DefinitionError(ValueError):
    def __init__(self, source: str, problem: str) -> None:
        super().__init__(f"{source}: {problem}")


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    name: str
    type: LogicalType
    description: str
    not_null: bool = False
    renamed_to: str | None = None

    @property
    def lake_name(self) -> str:
        """The column's name in the lake: `renamed_to` when the source name is misspelled."""
        return self.renamed_to or self.name


@dataclass(frozen=True, slots=True)
class EntityDefinition:
    name: str
    key: str
    description: str
    columns: tuple[ColumnDefinition, ...]
    audit_columns: tuple[ColumnDefinition, ...] = ()

    @property
    def all_columns(self) -> tuple[ColumnDefinition, ...]:
        """Every column in source order: the entity's own, then the audit columns."""
        return (*self.columns, *self.audit_columns)


@dataclass(frozen=True, slots=True)
class TableTemplate:
    """The description of a table a layer keeps per entity, such as bronze or its quarantine."""

    name: str
    description: str

    def describe(self, entity: EntityDefinition) -> str:
        return self.description.format(entity=entity.name, entity_description=entity.description)


COLUMN_FIELDS = frozenset({"type", "description"})
ENTITY_COLUMN_FIELDS = COLUMN_FIELDS | {"not_null", "renamed_to"}
ENTITY_FIELDS = frozenset({"name", "key", "description", "audit_columns", "columns"})
TEMPLATE_FIELDS = frozenset({"description"})
TEMPLATE_PLACEHOLDERS = frozenset({"entity", "entity_description"})


def _read(file: Traversable) -> tuple[str, dict[str, object]]:
    source = str(file)
    try:
        return source, tomllib.loads(file.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DefinitionError(source, "no such definition file") from error
    except tomllib.TOMLDecodeError as error:
        raise DefinitionError(source, f"invalid TOML: {error}") from error


def _table(source: str, fields: Fields, name: str) -> Fields:
    value = fields.get(name)
    if not isinstance(value, dict):
        raise DefinitionError(source, f"{name} must be a table")
    return value


def _text(source: str, fields: Fields, name: str, owner: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DefinitionError(source, f"{owner} needs a non-blank {name}")
    return value


def _flag(source: str, fields: Fields, name: str, owner: str) -> bool:
    value = fields.get(name, False)
    if not isinstance(value, bool):
        raise DefinitionError(source, f"{name} of {owner} must be true or false")
    return value


def _check_fields(source: str, fields: Fields, allowed: frozenset[str], owner: str) -> None:
    unknown = sorted(set(fields) - allowed)
    if unknown:
        raise DefinitionError(source, f"unknown fields of {owner}: {', '.join(unknown)}")


def _column(source: str, name: str, fields: object, allowed: frozenset[str]) -> ColumnDefinition:
    owner = f"column {name}"
    if not isinstance(fields, dict):
        raise DefinitionError(source, f"{owner} must be a table")
    _check_fields(source, fields, allowed, owner)
    type_name = _text(source, fields, "type", owner)
    if type_name not in LogicalType:
        expected = ", ".join(LogicalType)
        raise DefinitionError(source, f"unknown type {type_name!r} of {owner}; use {expected}")
    renamed_to = fields.get("renamed_to")
    if renamed_to is not None:
        renamed_to = _text(source, fields, "renamed_to", owner)
    return ColumnDefinition(
        name=name,
        type=LogicalType(type_name),
        description=_text(source, fields, "description", owner),
        not_null=_flag(source, fields, "not_null", owner),
        renamed_to=renamed_to,
    )


def _columns(source: str, tables: Fields, allowed: frozenset[str]) -> tuple[ColumnDefinition, ...]:
    return tuple(_column(source, name, fields, allowed) for name, fields in tables.items())


def _check_lake_names(source: str, columns: Iterable[ColumnDefinition]) -> None:
    seen: set[str] = set()
    for column in columns:
        if column.lake_name in seen:
            raise DefinitionError(source, f"two columns are named {column.lake_name} in the lake")
        seen.add(column.lake_name)


def load_columns(root: Traversable = DEFINITIONS) -> Mapping[str, ColumnDefinition]:
    """The columns the pipeline adds to lake tables (`columns.toml`), by name."""
    source, tables = _read(root / "columns.toml")
    return {column.name: column for column in _columns(source, tables, COLUMN_FIELDS)}


def load_audit_columns(root: Traversable = DEFINITIONS) -> tuple[ColumnDefinition, ...]:
    """The audit columns every source table ends with (`audit.toml`), in order."""
    source, tables = _read(root / "audit.toml")
    return _columns(source, tables, COLUMN_FIELDS)


def load_table_templates(root: Traversable = DEFINITIONS) -> Mapping[str, TableTemplate]:
    """The per-entity table descriptions of every layer (`tables.toml`), by table kind."""
    source, tables = _read(root / "tables.toml")
    templates: dict[str, TableTemplate] = {}
    for name in tables:
        fields = _table(source, tables, name)
        owner = f"table {name}"
        _check_fields(source, fields, TEMPLATE_FIELDS, owner)
        description = _text(source, fields, "description", owner)
        placeholders = {field for _, field, _, _ in string.Formatter().parse(description) if field}
        unknown = sorted(placeholders - TEMPLATE_PLACEHOLDERS)
        if unknown:
            raise DefinitionError(source, f"unknown placeholders of {owner}: {', '.join(unknown)}")
        templates[name] = TableTemplate(name=name, description=description)
    return templates


def _entity(file: Traversable, audit: tuple[ColumnDefinition, ...]) -> EntityDefinition:
    source, fields = _read(file)
    _check_fields(source, fields, ENTITY_FIELDS, "the entity")
    name = _text(source, fields, "name", "the entity")
    if f"{name}.toml" != file.name:
        raise DefinitionError(source, f"entity {name} must be defined in {name}.toml")
    columns = _columns(source, _table(source, fields, "columns"), ENTITY_COLUMN_FIELDS)
    if not columns:
        raise DefinitionError(source, f"entity {name} has no columns")
    key = _text(source, fields, "key", "the entity")
    if key not in {column.name for column in columns}:
        raise DefinitionError(source, f"key {key} is not a column of entity {name}")
    entity = EntityDefinition(
        name=name,
        key=key,
        description=_text(source, fields, "description", "the entity"),
        columns=columns,
        audit_columns=audit if _flag(source, fields, "audit_columns", "the entity") else (),
    )
    _check_lake_names(source, entity.all_columns)
    return entity


def load_entity(name: str, root: Traversable = DEFINITIONS) -> EntityDefinition:
    """One source table's definition, from `entities/<name>.toml`."""
    return _entity(root / "entities" / f"{name}.toml", load_audit_columns(root))


def load_entities(root: Traversable = DEFINITIONS) -> tuple[EntityDefinition, ...]:
    """Every source table's definition, ordered by name."""
    audit = load_audit_columns(root)
    entities = sorted(
        (file for file in (root / "entities").iterdir() if file.name.endswith(".toml")),
        key=lambda file: file.name,
    )
    return tuple(_entity(file, audit) for file in entities)
