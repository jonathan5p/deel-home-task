from pipelines.schemas.definitions import ColumnDefinition, ColumnType, EntityDefinition
from pipelines.schemas.gold_definitions import GoldTable

THING_ENTITY = EntityDefinition(
    name="things",
    key="thing_id",
    description="Things",
    columns=(
        ColumnDefinition("thing_id", ColumnType.LONG, "Thing identifier", not_null=True),
        ColumnDefinition("thing_nmae", ColumnType.STRING, "Name", renamed_to="thing_name"),
        ColumnDefinition("weight_kg", ColumnType.DECIMAL, "Weight"),
    ),
)


def column(name: str, column_type: ColumnType = ColumnType.LONG) -> ColumnDefinition:
    return ColumnDefinition(name, column_type, f"About {name}", not_null=name.startswith("_"))


DIM_THINGS = GoldTable(
    name="dim_things",
    description="Things, SCD2",
    columns=(
        column("thing_key"),
        column("thing_id"),
        column("thing_name", ColumnType.STRING),
        column("weight", ColumnType.DECIMAL),
        column("_commit_sequence"),
        column("_content_hash", ColumnType.STRING),
        column("_is_deleted", ColumnType.BOOLEAN),
        column("_valid_from", ColumnType.TIMESTAMP_MILLIS),
        column("_is_folded", ColumnType.BOOLEAN),
        column("_version_number"),
        column("_valid_to", ColumnType.TIMESTAMP_MILLIS),
        column("_is_current", ColumnType.BOOLEAN),
    ),
    references=(("thing_id", "things.thing_id"), ("thing_name", "things.thing_name")),
)
RENAMED = (("weight", "weight_kg"),)
SILVER_COLUMNS = (
    "thing_id",
    "thing_name",
    "weight_kg",
    "_commit_sequence",
    "_change_op",
    "_changed_at",
    "_content_hash",
    "_is_deleted",
)


def without(table: GoldTable, name: str) -> GoldTable:
    return GoldTable(
        name=table.name,
        description=table.description,
        columns=tuple(c for c in table.columns if c.name != name),
        references=tuple(r for r in table.references if r[0] != name),
    )


def referencing(table: GoldTable, *references: tuple[str, str]) -> GoldTable:
    return GoldTable(
        name=table.name,
        description=table.description,
        columns=table.columns,
        references=references,
    )
