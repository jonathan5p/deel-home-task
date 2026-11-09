from pipelines.bronze.columns import (
    COMMIT_SEQUENCE,
)
from pipelines.bronze.descriptions import (
    METADATA_COLUMN_DESCRIPTIONS,
    TableDescription,
    UndescribedColumnError,
    bronze_table_description,
    quarantine_table_description,
)
from pipelines.bronze.entities import ORDER_ITEMS


def test_bronze_tables_describe_business_columns_by_their_bronze_name() -> None:
    columns = bronze_table_description(ORDER_ITEMS).columns

    assert columns["quantity"] == "Number of units ordered (source column quanity)"
    assert "quanity" not in columns
    assert columns[COMMIT_SEQUENCE] == METADATA_COLUMN_DESCRIPTIONS[COMMIT_SEQUENCE]


def test_table_comments_name_the_source_table() -> None:
    assert "source table order_items" in bronze_table_description(ORDER_ITEMS).comment
    assert "source table order_items" in quarantine_table_description(ORDER_ITEMS).comment


def test_missing_and_blank_descriptions_are_undescribed() -> None:
    description = TableDescription(comment="Things", columns={"id": "Key", "name": " "})

    assert description.undescribed(["id", "name", "size"]) == ["name", "size"]


def test_the_error_names_the_table_and_its_undescribed_columns() -> None:
    error = UndescribedColumnError("lake.bronze.things", ["name", "size"])

    assert str(error) == "Columns of lake.bronze.things have no description: name, size"
