from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib.resources.abc import Traversable

from pipelines.schemas.definitions import (
    POSTGRES_TYPES,
    SCHEMAS,
    ColumnDefinition,
    ColumnType,
    EntityDefinition,
    TableTemplate,
    load_entities,
    load_table_templates,
)
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

REFERENCE = "reference"

# Spark SQL type names, spelled out because the docs site is built on the host, where PySpark is
# not installed; a unit test checks them against pipelines.lake.types.SPARK_TYPES.
LAKE_TYPES: Mapping[ColumnType, str] = {
    ColumnType.LONG: "bigint",
    ColumnType.INT: "int",
    ColumnType.STRING: "string",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.TIMESTAMP_MILLIS: "timestamp",
    ColumnType.DATE: "date",
    ColumnType.DECIMAL: "decimal(38,10)",
}

LAKE_NULLABLE = (
    "**Type** is the Spark SQL type. **Nullable** `no` means the pipeline always writes a value;"
    " Iceberg itself declares every lake column nullable."
)
INTRO = (
    "Every table and view of the lake and of gold, with its columns, types and descriptions."
    " The pages are rendered from the Avro schemas in `analytics/pipelines/src/pipelines/schemas/`"
    " each time the site is built, so they always match what the pipeline creates."
)


@dataclass(frozen=True, slots=True)
class Layer:
    name: str
    title: str
    summary: str
    intro: str


LAYERS: tuple[Layer, ...] = (
    Layer(
        name="bronze",
        title="Bronze",
        summary="Typed change events and quarantined records, in Iceberg",
        intro=(
            "Bronze holds one typed Iceberg table per source table, with one row per change event,"
            " and a quarantine table per source table for the records that fail validation."
            f" {LAKE_NULLABLE}"
        ),
    ),
    Layer(
        name="silver",
        title="Silver",
        summary="Deduplicated version history and its history and current views, in Iceberg",
        intro=(
            "Silver holds one append-only Iceberg table of versions per source table, and two"
            " views over it: `<entity>_history`, the versions that changed the row with their"
            f" validity range, and `<entity>_current`, the latest version. {LAKE_NULLABLE}"
        ),
    ),
    Layer(
        name="gold",
        title="Gold",
        summary="The dimensional model in PostgreSQL",
        intro=(
            "Gold lives in the analytics PostgreSQL. Each relation is a table in `gold_internal`"
            " with a view of the same name in `gold`, or a view in `gold` only. The public views"
            " leave out pipeline metadata such as `_commit_sequence` and `_is_deleted`;"
            " [the gold model](../../architecture/gold-model.md) describes what each view shows."
            " **Type** is the PostgreSQL type, and **Nullable** `no` means `NOT NULL`."
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class ReferenceColumn:
    name: str
    type: str
    nullable: bool
    description: str


@dataclass(frozen=True, slots=True)
class Relation:
    layer: str
    table: str
    description: str
    columns: tuple[ReferenceColumn, ...]
    notes: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return f"{self.layer}.{self.table}"


def _column(
    column: ColumnDefinition, name: str, types: Mapping[ColumnType, str]
) -> ReferenceColumn:
    return ReferenceColumn(
        name=name,
        type=types[column.type],
        nullable=not column.not_null,
        description=column.description,
    )


def lake_relations(
    layer: str, templates: Iterable[TableTemplate], entities: Iterable[EntityDefinition]
) -> tuple[Relation, ...]:
    """Every table and view of a lake layer, one per template of the layer and entity, by name."""
    entities = tuple(entities)
    relations = [
        Relation(
            layer=layer,
            table=template.table_name(entity),
            description=template.describe(entity),
            columns=tuple(
                _column(column, column.lake_name, LAKE_TYPES)
                for column in template.columns_for(entity)
            ),
            notes=(
                (f"Partitioned by day of `{template.partitioned_by}`.",)
                if template.partitioned_by
                else ()
            ),
        )
        for template in templates
        if template.layer == layer
        for entity in entities
    ]
    return tuple(sorted(relations, key=lambda relation: relation.table))


def gold_relations(tables: Mapping[str, GoldTable]) -> tuple[Relation, ...]:
    """Every gold relation by name. A relation whose view has its own description also shows the
    description of its internal table."""
    return tuple(
        Relation(
            layer="gold",
            table=table.name,
            description=table.view_description,
            columns=tuple(_column(column, column.name, POSTGRES_TYPES) for column in table.columns),
            notes=(
                (f"Internal table `gold_internal.{table.name}`: {table.description}",)
                if table.public_description
                else ()
            ),
        )
        for _, table in sorted(tables.items())
    )


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def relation_page(relation: Relation) -> str:
    """One relation: its description, notes, and a table of its columns."""
    lines = [f"# {relation.name}", "", _cell(relation.description), ""]
    for note in relation.notes:
        lines += [note, ""]
    lines += ["| Column | Type | Nullable | Description |", "| --- | --- | --- | --- |"]
    lines += [
        f"| `{column.name}` | {column.type} | {'yes' if column.nullable else 'no'}"
        f" | {_cell(column.description)} |"
        for column in relation.columns
    ]
    return "\n".join(lines) + "\n"


def layer_page(layer: Layer, relations: Iterable[Relation]) -> str:
    """A layer: its introduction and a linked list of its relations."""
    lines = [f"# {layer.title}", "", layer.intro, "", "| Relation | Description |", "| --- | --- |"]
    lines += [
        f"| [{relation.name}]({relation.table}.md) | {_cell(relation.description)} |"
        for relation in relations
    ]
    return "\n".join(lines) + "\n"


def index_page(relations_by_layer: Mapping[Layer, Sequence[Relation]]) -> str:
    """The reference's start page: one row per layer with its number of relations."""
    lines = ["# Table reference", "", INTRO, "", "| Layer | Relations | Holds |"]
    lines += ["| --- | --- | --- |"]
    lines += [
        f"| [{layer.title}]({layer.name}/index.md) | {len(relations)} | {layer.summary} |"
        for layer, relations in relations_by_layer.items()
    ]
    return "\n".join(lines) + "\n"


def reference_pages(root: Traversable = SCHEMAS) -> dict[str, str]:
    """Every page of the table reference, by its path in the docs site."""
    templates = tuple(load_table_templates(root).values())
    entities = load_entities(root)
    gold = gold_relations(load_gold_tables(root))
    relations_by_layer = {
        layer: gold if layer.name == "gold" else lake_relations(layer.name, templates, entities)
        for layer in LAYERS
    }
    pages = {f"{REFERENCE}/index.md": index_page(relations_by_layer)}
    for layer, relations in relations_by_layer.items():
        pages[f"{REFERENCE}/{layer.name}/index.md"] = layer_page(layer, relations)
        for relation in relations:
            pages[f"{REFERENCE}/{layer.name}/{relation.table}.md"] = relation_page(relation)
    return pages
