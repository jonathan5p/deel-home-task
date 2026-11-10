import pytest

pytest.importorskip("pyspark")

from pipelines.bronze.columns import RawColumn
from pipelines.bronze.entities import ORDER_ITEMS
from pipelines.bronze.tables import bronze_table, quarantine_table
from pipelines.lake.columns import MetadataColumn
from pipelines.schemas.definitions import load_table_templates


def test_bronze_tables_describe_entity_columns_by_their_lake_name() -> None:
    columns = bronze_table(ORDER_ITEMS).columns

    assert columns["quantity"].description == "Number of units ordered (source column quanity)"
    assert "quanity" not in columns
    assert columns[MetadataColumn.COMMIT_SEQUENCE] in load_table_templates()["bronze"].fields
    assert MetadataColumn.LANDING_FILE not in columns
    assert RawColumn.KEY not in columns


def test_table_comments_come_from_the_templates_and_name_the_source_table() -> None:
    bronze = bronze_table(ORDER_ITEMS).comment
    quarantine = quarantine_table(ORDER_ITEMS).comment

    assert bronze.startswith(f"{ORDER_ITEMS.definition.description}. Change log of source table")
    assert "source table order_items" in quarantine


def test_quarantine_tables_describe_the_raw_kafka_columns() -> None:
    columns = quarantine_table(ORDER_ITEMS).columns

    assert columns[RawColumn.KEY].description == "Raw Kafka record key, as text"
    assert "quantity" not in columns
    assert MetadataColumn.COMMIT_SEQUENCE not in columns


def test_bronze_and_quarantine_tables_are_partitioned_by_publish_day() -> None:
    assert bronze_table(ORDER_ITEMS).partitioned_by == MetadataColumn.PUBLISHED_AT
    assert quarantine_table(ORDER_ITEMS).partitioned_by == MetadataColumn.PUBLISHED_AT
