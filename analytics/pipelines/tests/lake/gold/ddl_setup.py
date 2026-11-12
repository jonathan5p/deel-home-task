from pipelines.schemas.definitions import ColumnDefinition, ColumnType
from pipelines.schemas.gold_definitions import GoldTable

THINGS = GoldTable(
    name="things",
    description="Things, one row per thing",
    public_description="Things nobody deleted",
    columns=(
        ColumnDefinition("thing_id", ColumnType.LONG, "Thing identifier", not_null=True),
        ColumnDefinition("label", ColumnType.STRING, "The thing's label"),
        ColumnDefinition("_commit_sequence", ColumnType.LONG, "Commit position", not_null=True),
        ColumnDefinition("_is_deleted", ColumnType.BOOLEAN, "Deleted", not_null=True),
    ),
)
VALID_TO = ColumnDefinition("_valid_to", ColumnType.TIMESTAMP_MILLIS, "Replaced at")
