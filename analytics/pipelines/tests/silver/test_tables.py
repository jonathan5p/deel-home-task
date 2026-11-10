import pytest

pytest.importorskip("pyspark")

from pipelines.bronze.entities import ORDER_ITEMS
from pipelines.lake.columns import MetadataColumn
from pipelines.silver.columns import SILVER_METADATA_COLUMNS
from pipelines.silver.tables import silver_table


def test_silver_tables_describe_the_entity_columns_then_the_version_columns() -> None:
    columns = silver_table(ORDER_ITEMS).columns

    assert list(columns) == [
        *(column.lake_name for column in ORDER_ITEMS.columns),
        *SILVER_METADATA_COLUMNS,
    ]
    assert columns["quantity"].description == "Number of units ordered (source column quanity)"


def test_the_table_comment_names_the_source_table_and_its_version_history() -> None:
    comment = silver_table(ORDER_ITEMS).comment

    assert comment.startswith(
        f"{ORDER_ITEMS.definition.description}. Append-only version history of source table"
        " order_items"
    )


def test_silver_tables_are_partitioned_by_change_day() -> None:
    assert silver_table(ORDER_ITEMS).partitioned_by == MetadataColumn.CHANGED_AT
