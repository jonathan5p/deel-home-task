from collections.abc import Mapping
from dataclasses import dataclass, replace
from importlib.resources.abc import Traversable

from pipelines.schemas.definitions import (
    FIELD_PROPERTIES,
    RECORD_PROPERTIES,
    SCHEMAS,
    ColumnDefinition,
    DefinitionError,
    _check_unique,
    _record_header,
    _schema_files,
    _text,
    _typed_field,
    load_entities,
)

GOLD_RECORD_PROPERTIES = RECORD_PROPERTIES | {"public_doc"}
GOLD_FIELD_PROPERTIES = FIELD_PROPERTIES | {"from", "view_only"}
GOLD = "gold"


@dataclass(frozen=True, slots=True)
class GoldTable:
    """A gold relation: a table in gold_internal with its public view in gold, or a view only.

    `view_only` names the columns only the public view has, which it computes when it is read,
    such as the attributes of the dimension version valid when a fact changed.
    """

    name: str
    description: str
    columns: tuple[ColumnDefinition, ...]
    public_description: str | None = None
    references: tuple[tuple[str, str], ...] = ()
    view_only: tuple[str, ...] = ()

    @property
    def view_description(self) -> str:
        return self.public_description or self.description

    def column(self, name: str) -> ColumnDefinition:
        return {column.name: column for column in self.columns}[name]

    def stored(self) -> GoldTable:
        """The relation as the table holds it, without the view-only columns."""
        hidden = set(self.view_only)
        columns = tuple(column for column in self.columns if column.name not in hidden)
        references = tuple((name, ref) for name, ref in self.references if name not in hidden)
        return replace(self, columns=columns, references=references, view_only=())

    def reference(self, name: str) -> str | None:
        """The `from` of a column: `<source table>.<lake name>` or `gold.<table>.<column>`."""
        return dict(self.references).get(name)


@dataclass(frozen=True, slots=True)
class _Field:
    column: ColumnDefinition
    reference: str | None
    view_only: bool = False


def _field(source: str, field: object) -> _Field:
    typed = _typed_field(source, field, GOLD_FIELD_PROPERTIES)
    properties, owner = typed.properties, typed.owner
    if ("doc" in properties) == ("from" in properties):
        raise DefinitionError(source, f"{owner} needs either a doc or a from, not both")
    reference = _text(source, properties, "from", owner) if "from" in properties else None
    description = "" if reference else _text(source, properties, "doc", owner)
    view_only = properties.get("view_only", False)
    if not isinstance(view_only, bool):
        raise DefinitionError(source, f"view_only of {owner} must be true or false")
    column = ColumnDefinition(
        name=typed.name, type=typed.type, description=description, not_null=not typed.nullable
    )
    return _Field(column=column, reference=reference, view_only=view_only)


@dataclass(frozen=True, slots=True)
class _GoldRecord:
    source: str
    table: GoldTable
    fields: tuple[_Field, ...]


def _gold_record(file: Traversable) -> _GoldRecord:
    source, properties, name = _record_header(file, GOLD_RECORD_PROPERTIES)
    owner = f"record {name}"
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
        view_only=tuple(field.column.name for field in fields if field.view_only),
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
    references = tuple(
        (field.column.name, field.reference)
        for field in record.fields
        if field.reference is not None
    )
    return replace(record.table, columns=tuple(columns), references=references)


def load_gold_tables(root: Traversable = SCHEMAS) -> Mapping[str, GoldTable]:
    """The gold relations (`gold/<name>.avsc`) by name, each with its columns in field order.

    A field takes its description either from its own `doc` or, with `from`, from a source
    column (by its lake name) or a gold field that has its own `doc`.
    """
    files = _schema_files(root / GOLD)
    records = {record.table.name: record for record in map(_gold_record, files)}
    referable = _referable_columns(root, records)
    return {name: _resolved(record, referable) for name, record in records.items()}
