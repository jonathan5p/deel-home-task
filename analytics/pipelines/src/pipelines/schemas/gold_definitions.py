from collections.abc import Mapping
from dataclasses import dataclass, replace
from importlib.resources.abc import Traversable

from pipelines.schemas.definitions import (
    FIELD_PROPERTIES,
    RECORD_PROPERTIES,
    SCHEMA_SUFFIX,
    SCHEMAS,
    ColumnDefinition,
    DefinitionError,
    _check_properties,
    _check_unique,
    _column_type,
    _read,
    _schema_files,
    _text,
    load_entities,
)

GOLD_RECORD_PROPERTIES = RECORD_PROPERTIES | {"public_doc"}
GOLD_FIELD_PROPERTIES = FIELD_PROPERTIES | {"from"}
GOLD = "gold"


@dataclass(frozen=True, slots=True)
class GoldTable:
    """A gold relation: a table in gold_internal with its public view in gold, or a view only."""

    name: str
    description: str
    columns: tuple[ColumnDefinition, ...]
    public_description: str | None = None

    @property
    def view_description(self) -> str:
        return self.public_description or self.description

    def column(self, name: str) -> ColumnDefinition:
        return {column.name: column for column in self.columns}[name]


@dataclass(frozen=True, slots=True)
class _Field:
    column: ColumnDefinition
    reference: str | None


def _field(source: str, field: object) -> _Field:
    if not isinstance(field, dict):
        raise DefinitionError(source, "every field must be a JSON object")
    name = _text(source, field, "name", "a field")
    owner = f"field {name}"
    _check_properties(source, field, GOLD_FIELD_PROPERTIES, owner)
    column_type, nullable = _column_type(source, field.get("type"), owner)
    if nullable and ("default" not in field or field["default"] is not None):
        raise DefinitionError(source, f'nullable {owner} needs "default": null')
    if not nullable and "default" in field:
        raise DefinitionError(source, f"{owner} is not nullable and takes no default")
    if ("doc" in field) == ("from" in field):
        raise DefinitionError(source, f"{owner} needs either a doc or a from, not both")
    reference = _text(source, field, "from", owner) if "from" in field else None
    description = "" if reference else _text(source, field, "doc", owner)
    column = ColumnDefinition(
        name=name, type=column_type, description=description, not_null=not nullable
    )
    return _Field(column=column, reference=reference)


@dataclass(frozen=True, slots=True)
class _GoldRecord:
    source: str
    table: GoldTable
    fields: tuple[_Field, ...]


def _gold_record(file: Traversable) -> _GoldRecord:
    source, properties = _read(file)
    if properties.get("type") != "record":
        raise DefinitionError(source, 'the schema must be of "type": "record"')
    name = _text(source, properties, "name", "the record")
    owner = f"record {name}"
    if f"{name}{SCHEMA_SUFFIX}" != file.name:
        raise DefinitionError(source, f"{owner} must be defined in {name}{SCHEMA_SUFFIX}")
    _check_properties(source, properties, GOLD_RECORD_PROPERTIES, owner)
    raw_fields = properties.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise DefinitionError(source, f"fields of {owner} must be a non-empty list")
    fields = tuple(_field(source, field) for field in raw_fields)
    _check_unique(source, (field.column.name for field in fields), "fields")
    public = _text(source, properties, "public_doc", owner) if "public_doc" in properties else None
    table = GoldTable(
        name=name,
        description=_text(source, properties, "doc", owner),
        columns=tuple(field.column for field in fields),
        public_description=public,
    )
    return _GoldRecord(source=source, table=table, fields=fields)


def _referable_columns(
    root: Traversable, records: Mapping[str, _GoldRecord]
) -> Mapping[str, ColumnDefinition]:
    """Every column a gold field may take its description from, by `<owner>.<column>`."""
    columns: dict[str, ColumnDefinition] = {}
    for entity in load_entities(root):
        for column in entity.all_columns:
            columns[f"{entity.name}.{column.lake_name}"] = column
    for record in records.values():
        for field in record.fields:
            if field.reference is None:
                columns[f"{GOLD}.{record.table.name}.{field.column.name}"] = field.column
    return columns


def _resolved(record: _GoldRecord, referable: Mapping[str, ColumnDefinition]) -> GoldTable:
    columns: list[ColumnDefinition] = []
    for field in record.fields:
        column = field.column
        if field.reference is not None:
            owner = f"field {column.name}"
            referenced = referable.get(field.reference)
            if referenced is None:
                raise DefinitionError(
                    record.source,
                    f"from {field.reference} of {owner} names no column with its own doc;"
                    f" use <source table>.<column> or gold.<table>.<column>",
                )
            if referenced.type is not column.type:
                raise DefinitionError(
                    record.source,
                    f"{owner} is {column.type} but {field.reference} is {referenced.type}",
                )
            column = replace(column, description=referenced.description)
        columns.append(column)
    return replace(record.table, columns=tuple(columns))


def load_gold_tables(root: Traversable = SCHEMAS) -> Mapping[str, GoldTable]:
    """The gold relations (`gold/<name>.avsc`) by name, each with its columns in field order.

    A field takes its description either from its own `doc` or, with `from`, from a source
    column (by its lake name) or a gold field that has its own `doc`.
    """
    files = _schema_files(root / GOLD)
    records = {record.table.name: record for record in map(_gold_record, files)}
    referable = _referable_columns(root, records)
    return {name: _resolved(record, referable) for name, record in records.items()}
