from analytics.pipelines.tests.schemas.render_setup import PRICE, THING_ID
from pipelines.schemas.definitions import (
    ColumnDefinition,
    ColumnGroup,
    ColumnType,
    TableTemplate,
)
from pipelines.schemas.gold_definitions import GoldTable
from pipelines.schemas.reference import Layer, ReferenceColumn, Relation

CHANGED_AT = ColumnDefinition(
    "_changed_at", ColumnType.TIMESTAMP_MILLIS, "Commit time | in the source", not_null=True
)
BRONZE = TableTemplate(
    name="bronze",
    description="Changes of {entity}",
    include=(ColumnGroup("source"),),
    fields=(CHANGED_AT,),
    partitioned_by="_changed_at",
)

DIM_THING = GoldTable(
    name="dim_things",
    description="Things, deleted ones included",
    columns=(THING_ID, PRICE),
    public_description="Things not deleted in the source",
)
THING_FRESHNESS = GoldTable(
    name="thing_freshness",
    description="Newest change of things",
    columns=(CHANGED_AT,),
)

LAYER = Layer(name="bronze", title="Bronze", summary="Typed changes", intro="Bronze intro.")
THINGS_RELATION = Relation(
    layer="bronze",
    table="things",
    description="Changes | of things",
    columns=(
        ReferenceColumn("thing_id", "bigint", nullable=False, description="Identifier"),
        ReferenceColumn("thing_name", "string", nullable=True, description="Name"),
    ),
    notes=("Partitioned by day of `_changed_at`.",),
)
THINGS_PAGE = """# bronze.things

Changes \\| of things

Partitioned by day of `_changed_at`.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `thing_id` | bigint | no | Identifier |
| `thing_name` | string | yes | Name |
"""
LAYER_PAGE = """# Bronze

Bronze intro.

| Relation | Description |
| --- | --- |
| [bronze.things](things.md) | Changes \\| of things |
"""
