import pytest

from common.definitions import load_columns
from pipelines.bronze.columns import (
    BRONZE_METADATA_COLUMNS,
    QUARANTINE_COLUMNS,
    SOURCE_COORDINATES,
    MetadataColumn,
    RawColumn,
)


@pytest.mark.parametrize("column", [*MetadataColumn, *RawColumn], ids=str)
def test_every_column_the_job_writes_is_defined(column: str) -> None:
    assert column in load_columns()


def test_metadata_columns_start_with_an_underscore_and_raw_kafka_ones_do_not() -> None:
    assert all(column.startswith("_") for column in MetadataColumn)
    assert not any(column.startswith("_") for column in RawColumn)


def test_bronze_and_quarantine_tables_both_start_with_the_source_coordinates() -> None:
    assert BRONZE_METADATA_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
    assert QUARANTINE_COLUMNS[: len(SOURCE_COORDINATES)] == SOURCE_COORDINATES
