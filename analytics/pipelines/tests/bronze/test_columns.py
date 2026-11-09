import pytest

from pipelines.bronze.columns import (
    BRONZE_METADATA_COLUMNS,
    QUARANTINE_COLUMNS,
    SOURCE_COORDINATES,
    MetadataColumn,
    RawColumn,
)
from pipelines.schemas.definitions import load_metadata_columns, load_table_templates


@pytest.mark.parametrize("column", [*MetadataColumn, *RawColumn], ids=str)
def test_every_column_the_job_writes_is_defined_in_the_metadata_schema(column: str) -> None:
    assert column in load_metadata_columns()


def test_each_table_schema_lists_the_metadata_columns_the_job_writes_to_it() -> None:
    templates = load_table_templates()

    assert [column.name for column in templates["bronze"].metadata_columns] == [
        *BRONZE_METADATA_COLUMNS
    ]
    assert [column.name for column in templates["bronze_quarantine"].metadata_columns] == [
        *QUARANTINE_COLUMNS,
        MetadataColumn.QUARANTINED_AT,
    ]


def test_metadata_columns_start_with_an_underscore_and_raw_kafka_ones_do_not() -> None:
    assert all(column.startswith("_") for column in MetadataColumn)
    assert not any(column.startswith("_") for column in RawColumn)


def test_bronze_and_quarantine_tables_both_start_with_the_source_coordinates() -> None:
    assert BRONZE_METADATA_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
    assert QUARANTINE_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
