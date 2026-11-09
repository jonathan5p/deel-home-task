import json
import string
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from importlib.resources import files
from importlib.resources.abc import Traversable

SCHEMAS: Traversable = files("pipelines.schemas")
SCHEMA_SUFFIX = ".avsc"

type Properties = Mapping[str, object]


class ColumnType(StrEnum):
    """The column types the schemas may use, named after their Avro type or logical type."""

    LONG = "long"
    INT = "int"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP_MILLIS = "timestamp-millis"
    DATE = "date"
    DECIMAL = "decimal"


AVRO_TYPES: Mapping[ColumnType, object] = {
    ColumnType.LONG: "long",
    ColumnType.INT: "int",
    ColumnType.STRING: "string",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.TIMESTAMP_MILLIS: {"type": "long", "logicalType": "timestamp-millis"},
    ColumnType.DATE: {"type": "int", "logicalType": "date"},
    # The source DECIMAL declares no precision; the lake stores every decimal as DECIMAL(38,10).
    ColumnType.DECIMAL: {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 10},
}


class DefinitionError(ValueError):
    def __init__(self, source: str, problem: str) -> None:
        super().__init__(f"{source}: {problem}")


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    name: str
    type: ColumnType
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
    included_columns: tuple[ColumnDefinition, ...] = ()

    @property
    def all_columns(self) -> tuple[ColumnDefinition, ...]:
        """Every column in source order: the entity's own, then the included shared ones."""
        return (*self.columns, *self.included_columns)


SOURCE_GROUP = "source"
TEMPLATE_SUFFIX = "_template"


@dataclass(frozen=True, slots=True)
class ColumnGroup:
    """An `include` token of a table template, standing for some of the entity's columns.

    `source` is the entity's own fields; the name of a shared schema (such as `audit`) is that
    schema's fields, which only entities that include it have.
    """

    name: str
    shared_columns: tuple[ColumnDefinition, ...] = ()

    def expand(self, entity: EntityDefinition) -> tuple[ColumnDefinition, ...]:
        if self.name == SOURCE_GROUP:
            return entity.columns
        return tuple(column for column in self.shared_columns if column in entity.included_columns)


@dataclass(frozen=True, slots=True)
class TableTemplate:
    """A table a layer keeps per entity, such as bronze or its quarantine.

    `name` is the template's file name without `_template`: the layer, then the kind of table
    if the layer keeps more than one per entity (`bronze`, `bronze_quarantine`). The table's
    columns are the included groups, in order, then the template's own `fields`.
    """

    name: str
    description: str
    include: tuple[ColumnGroup, ...] = ()
    fields: tuple[ColumnDefinition, ...] = ()

    @property
    def layer(self) -> str:
        return self.name.split("_", 1)[0]

    def describe(self, entity: EntityDefinition) -> str:
        return self.description.format(entity=entity.name, entity_description=entity.description)

    def table_name(self, entity: EntityDefinition) -> str:
        """The entity's table in the layer: `orders` for bronze, `orders_quarantine` for its
        quarantine."""
        return entity.name + self.name.removeprefix(self.layer)

    def columns_for(self, entity: EntityDefinition) -> tuple[ColumnDefinition, ...]:
        """The table's columns for `entity`, in order: the included groups, then `fields`."""
        included = (column for group in self.include for column in group.expand(entity))
        return (*included, *self.fields)


RECORD_PROPERTIES = frozenset({"type", "name", "namespace", "doc", "fields"})
SOURCE_PROPERTIES = RECORD_PROPERTIES | {"key", "include"}
TABLE_PROPERTIES = RECORD_PROPERTIES | {"include"}
FIELD_PROPERTIES = frozenset({"name", "type", "doc", "default"})
SOURCE_FIELD_PROPERTIES = FIELD_PROPERTIES | {"renamed_to"}
TEMPLATE_PLACEHOLDERS = frozenset({"entity", "entity_description"})


@dataclass(frozen=True, slots=True)
class _Record:
    source: str
    properties: Properties
    name: str
    description: str
    columns: tuple[ColumnDefinition, ...]


def _read(file: Traversable) -> tuple[str, Properties]:
    source = str(file)
    try:
        schema = json.loads(file.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DefinitionError(source, "no such schema file") from error
    except json.JSONDecodeError as error:
        raise DefinitionError(source, f"invalid JSON: {error}") from error
    if not isinstance(schema, dict):
        raise DefinitionError(source, "the schema must be a JSON object")
    return source, schema


def _text(source: str, properties: Properties, name: str, owner: str) -> str:
    value = properties.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DefinitionError(source, f"{owner} needs a non-blank {name}")
    return value


def _names(source: str, properties: Properties, name: str, owner: str) -> tuple[str, ...]:
    value = properties.get(name, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise DefinitionError(source, f"{name} of {owner} must be a list of names")
    return tuple(str(item) for item in value)


def _check_properties(
    source: str, properties: Properties, allowed: frozenset[str], owner: str
) -> None:
    unknown = sorted(set(properties) - allowed)
    if unknown:
        raise DefinitionError(source, f"unknown properties of {owner}: {', '.join(unknown)}")


def _check_unique(source: str, names: Iterable[str], what: str) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise DefinitionError(source, f"two {what} are named {name}")
        seen.add(name)


def _column_type(source: str, avro_type: object, owner: str) -> tuple[ColumnType, bool]:
    """The column type and nullability of an Avro field type: `["null", T]` is nullable."""
    nullable = isinstance(avro_type, list) and len(avro_type) == 2 and avro_type[0] == "null"
    plain = avro_type[1] if nullable and isinstance(avro_type, list) else avro_type
    for column_type, avro in AVRO_TYPES.items():
        if plain == avro:
            return column_type, nullable
    expected = "; ".join(json.dumps(avro) for avro in AVRO_TYPES.values())
    raise DefinitionError(
        source,
        f"unsupported type {json.dumps(avro_type)} of {owner}; use one of {expected},"
        ' or ["null", <type>] for a nullable column',
    )


def _column(source: str, field: object, allowed: frozenset[str]) -> ColumnDefinition:
    if not isinstance(field, dict):
        raise DefinitionError(source, "every field must be a JSON object")
    name = _text(source, field, "name", "a field")
    owner = f"field {name}"
    _check_properties(source, field, allowed, owner)
    column_type, nullable = _column_type(source, field.get("type"), owner)
    if nullable and ("default" not in field or field["default"] is not None):
        raise DefinitionError(source, f'nullable {owner} needs "default": null')
    if not nullable and "default" in field:
        raise DefinitionError(source, f"{owner} is not nullable and takes no default")
    renamed_to = field.get("renamed_to")
    if renamed_to is not None:
        renamed_to = _text(source, field, "renamed_to", owner)
    return ColumnDefinition(
        name=name,
        type=column_type,
        description=_text(source, field, "doc", owner),
        not_null=not nullable,
        renamed_to=renamed_to,
    )


def _record(file: Traversable, allowed: frozenset[str], field_allowed: frozenset[str]) -> _Record:
    source, properties = _read(file)
    if properties.get("type") != "record":
        raise DefinitionError(source, 'the schema must be of "type": "record"')
    name = _text(source, properties, "name", "the record")
    owner = f"record {name}"
    if f"{name}{SCHEMA_SUFFIX}" != file.name:
        raise DefinitionError(source, f"{owner} must be defined in {name}{SCHEMA_SUFFIX}")
    _check_properties(source, properties, allowed, owner)
    fields = properties.get("fields")
    if not isinstance(fields, list):
        raise DefinitionError(source, f"fields of {owner} must be a list")
    columns = tuple(_column(source, field, field_allowed) for field in fields)
    _check_unique(source, (column.name for column in columns), "fields")
    return _Record(
        source=source,
        properties=properties,
        name=name,
        description=_text(source, properties, "doc", owner),
        columns=columns,
    )


def _schema_files(folder: Traversable) -> list[Traversable]:
    schemas = (file for file in folder.iterdir() if file.name.endswith(SCHEMA_SUFFIX))
    return sorted(schemas, key=lambda file: file.name)


def load_shared_columns(name: str, root: Traversable = SCHEMAS) -> tuple[ColumnDefinition, ...]:
    """The fields of a shared record (`<name>.avsc`, such as `audit`), in order."""
    return _record(root / f"{name}{SCHEMA_SUFFIX}", RECORD_PROPERTIES, FIELD_PROPERTIES).columns


def _include_group(name: str, root: Traversable) -> ColumnGroup | None:
    if name == SOURCE_GROUP:
        return ColumnGroup(name)
    if name in {file.name.removesuffix(SCHEMA_SUFFIX) for file in _schema_files(root)}:
        return ColumnGroup(name, load_shared_columns(name, root))
    return None


def _template(file: Traversable, root: Traversable) -> TableTemplate:
    record = _record(file, TABLE_PROPERTIES, FIELD_PROPERTIES)
    owner = f"record {record.name}"
    if not record.name.endswith(TEMPLATE_SUFFIX):
        raise DefinitionError(record.source, f"{owner} must be named <table>{TEMPLATE_SUFFIX}")
    placeholders = {
        field for _, field, _, _ in string.Formatter().parse(record.description) if field
    }
    unknown = sorted(placeholders - TEMPLATE_PLACEHOLDERS)
    if unknown:
        raise DefinitionError(
            record.source, f"unknown placeholders of {owner}: {', '.join(unknown)}"
        )
    names = _names(record.source, record.properties, "include", owner)
    _check_unique(record.source, names, "includes")
    groups = {name: _include_group(name, root) for name in names}
    undefined = [name for name, group in groups.items() if group is None]
    if undefined:
        raise DefinitionError(
            record.source,
            f"unknown include of {owner}: {', '.join(undefined)};"
            f" use {SOURCE_GROUP} or the name of a shared schema such as audit",
        )
    if not names and not record.columns:
        raise DefinitionError(record.source, f"{owner} has no columns")
    return TableTemplate(
        name=record.name.removesuffix(TEMPLATE_SUFFIX),
        description=record.description,
        include=tuple(group for group in groups.values() if group is not None),
        fields=record.columns,
    )


def load_table_templates(root: Traversable = SCHEMAS) -> Mapping[str, TableTemplate]:
    """The tables each layer keeps per entity (`tables/<name>_template.avsc`), by name."""
    templates = (_template(file, root) for file in _schema_files(root / "tables"))
    return {template.name: template for template in templates}


def _entity(file: Traversable, root: Traversable) -> EntityDefinition:
    record = _record(file, SOURCE_PROPERTIES, SOURCE_FIELD_PROPERTIES)
    owner = f"record {record.name}"
    if not record.columns:
        raise DefinitionError(record.source, f"{owner} has no fields")
    key = _text(record.source, record.properties, "key", owner)
    if key not in {column.name for column in record.columns}:
        raise DefinitionError(record.source, f"key {key} is not a field of {owner}")
    included = tuple(
        column
        for name in _names(record.source, record.properties, "include", owner)
        for column in load_shared_columns(name, root)
    )
    entity = EntityDefinition(
        name=record.name,
        key=key,
        description=record.description,
        columns=record.columns,
        included_columns=included,
    )
    lake_names = (column.lake_name for column in entity.all_columns)
    _check_unique(record.source, lake_names, "columns in the lake")
    return entity


def load_entity(name: str, root: Traversable = SCHEMAS) -> EntityDefinition:
    """One source table's definition, from `sources/<name>.avsc`."""
    return _entity(root / "sources" / f"{name}{SCHEMA_SUFFIX}", root)


def load_entities(root: Traversable = SCHEMAS) -> tuple[EntityDefinition, ...]:
    """Every source table's definition, ordered by name."""
    return tuple(_entity(file, root) for file in _schema_files(root / "sources"))
