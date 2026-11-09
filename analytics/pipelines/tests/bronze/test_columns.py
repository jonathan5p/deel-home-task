import pytest

from pipelines.bronze.columns import (
    BRONZE_METADATA_COLUMNS,
    QUARANTINE_COLUMNS,
    SOURCE_COORDINATES,
    MetadataColumn,
    RawColumn,
)
from pipelines.schemas.definitions import load_table_templates

TEMPLATES = load_table_templates()


@pytest.mark.parametrize("column", [*MetadataColumn, *RawColumn], ids=str)
def test_every_column_the_job_writes_is_defined_in_a_table_template(column: str) -> None:
    assert any(column in [field.name for field in t.fields] for t in TEMPLATES.values())


def test_each_table_template_defines_exactly_the_columns_the_job_writes_to_it() -> None:
    assert [field.name for field in TEMPLATES["bronze"].fields] == [*BRONZE_METADATA_COLUMNS]
    assert [field.name for field in TEMPLATES["bronze_quarantine"].fields] == [
        *QUARANTINE_COLUMNS,
        MetadataColumn.QUARANTINED_AT,
    ]


def test_metadata_columns_start_with_an_underscore_and_raw_kafka_ones_do_not() -> None:
    assert all(column.startswith("_") for column in MetadataColumn)
    assert not any(column.startswith("_") for column in RawColumn)


def test_bronze_and_quarantine_tables_both_start_with_the_source_coordinates() -> None:
    assert BRONZE_METADATA_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
    assert QUARANTINE_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
