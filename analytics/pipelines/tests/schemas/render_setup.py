from pathlib import Path

from pipelines.schemas.definitions import (
    SCHEMA_SUFFIX,
    ColumnDefinition,
    ColumnGroup,
    ColumnType,
    EntityDefinition,
    TableTemplate,
)
from pipelines.schemas.render import Schema, render_tables

THING_ID = ColumnDefinition("thing_id", ColumnType.LONG, "Thing identifier", not_null=True)
THING_NAME = ColumnDefinition("thing_nmae", ColumnType.STRING, "Name", renamed_to="thing_name")
THINGS = EntityDefinition(
    name="things", key="thing_id", description="Things", columns=(THING_ID, THING_NAME)
)
PRICE = ColumnDefinition("_price", ColumnType.DECIMAL, "Price", not_null=True)
SILVER = TableTemplate(
    name="silver",
    description="Versions of {entity}: {entity_description}",
    include=(ColumnGroup("source"),),
    fields=(PRICE,),
)
SILVER_ARCHIVE = TableTemplate(name="silver_archive", description="Old {entity}", fields=(PRICE,))


def schema_files(root: Path) -> dict[str, str]:
    """The text of every schema file under `root`, by its path relative to `root`."""
    files = sorted(root.rglob(f"*{SCHEMA_SUFFIX}"))
    return {file.relative_to(root).as_posix(): file.read_text(encoding="utf-8") for file in files}


def silver_schemas() -> dict[str, Schema]:
    return render_tables([SILVER, SILVER_ARCHIVE], [THINGS])
