import pytest

from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.silver.columns import COPIED_METADATA_COLUMNS, SILVER_METADATA_COLUMNS
from pipelines.schemas.definitions import ColumnDefinition, ColumnType, load_table_templates

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


def test_the_history_template_adds_the_validity_columns_to_the_silver_columns() -> None:
    history = TEMPLATES["silver_history"]

    assert [field.name for field in history.fields] == [
        *SILVER_METADATA_COLUMNS,
        MetadataColumn.VALID_FROM,
        MetadataColumn.VALID_TO,
        MetadataColumn.IS_CURRENT,
    ]
    assert [group.name for group in history.include] == ["source", "audit"]


def test_the_current_template_drops_the_columns_its_filter_makes_constant() -> None:
    current = TEMPLATES["silver_current"]

    assert [field.name for field in current.fields] == [
        *COPIED_METADATA_COLUMNS,
        MetadataColumn.VALID_FROM,
    ]
    assert [group.name for group in current.include] == ["source", "audit"]


@pytest.mark.parametrize("view", ["silver_history", "silver_current"])
def test_the_view_columns_shared_with_the_silver_table_are_defined_as_in_it(view: str) -> None:
    silver = {field.name: field for field in TEMPLATES["silver"].fields}

    shared = [field for field in TEMPLATES[view].fields if field.name in silver]

    assert shared
    assert shared == [silver[field.name] for field in shared]


def test_the_validity_columns_are_timestamps_and_a_flag() -> None:
    fields = {field.name: field for field in TEMPLATES["silver_history"].fields}

    assert fields[MetadataColumn.VALID_FROM] == ColumnDefinition(
        name=MetadataColumn.VALID_FROM,
        type=ColumnType.TIMESTAMP_MILLIS,
        description=(
            "Time this version of the row became current: the change's commit time in the source"
        ),
        not_null=True,
    )
    assert fields[MetadataColumn.VALID_TO] == ColumnDefinition(
        name=MetadataColumn.VALID_TO,
        type=ColumnType.TIMESTAMP_MILLIS,
        description=(
            "Time the next version of the row replaced this one; null while this version is current"
        ),
    )
    assert fields[MetadataColumn.IS_CURRENT] == ColumnDefinition(
        name=MetadataColumn.IS_CURRENT,
        type=ColumnType.BOOLEAN,
        description="Whether this is the latest version of the row",
        not_null=True,
    )
