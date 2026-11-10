from pipelines.lake.columns import MetadataColumn
from pipelines.schemas.definitions import ColumnDefinition, ColumnType, load_table_templates
from pipelines.silver.columns import COPIED_METADATA_COLUMNS, SILVER_METADATA_COLUMNS

TEMPLATES = load_table_templates()


def test_the_silver_template_defines_exactly_the_columns_the_job_adds() -> None:
    assert [field.name for field in TEMPLATES["silver"].fields] == [*SILVER_METADATA_COLUMNS]
    assert [group.name for group in TEMPLATES["silver"].include] == ["source", "audit"]


def test_the_copied_change_metadata_is_defined_as_in_bronze() -> None:
    bronze = {field.name: field for field in TEMPLATES["bronze"].fields}
    silver = {field.name: field for field in TEMPLATES["silver"].fields}

    assert all(silver[column] == bronze[column] for column in COPIED_METADATA_COLUMNS)


def test_a_version_marks_a_source_delete_with_a_boolean() -> None:
    (is_deleted,) = (
        field for field in TEMPLATES["silver"].fields if field.name == MetadataColumn.IS_DELETED
    )

    assert is_deleted == ColumnDefinition(
        name=MetadataColumn.IS_DELETED,
        type=ColumnType.BOOLEAN,
        description=(
            "Whether the row was deleted in the source; the version keeps its last known values"
        ),
        not_null=True,
    )
