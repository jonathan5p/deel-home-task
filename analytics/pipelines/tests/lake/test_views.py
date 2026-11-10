from dataclasses import replace

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.tables_setup import stored_description
from analytics.pipelines.tests.lake.views_setup import (
    even_numbers,
    numbers_table,
    view_metadata_file,
)
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.lake.views import (
    StoredView,
    ViewDefinition,
    create_view_statement,
    ensure_view,
    stored_view,
    view_definition,
)
from pipelines.schemas.definitions import ColumnDefinition, ColumnType, load_entity


def test_a_view_definition_takes_the_template_description_and_columns_by_lake_name() -> None:
    order_items = load_entity("order_items")

    definition = view_definition("silver_current", order_items, "SELECT 1")

    template = TABLE_TEMPLATES["silver_current"]
    assert definition.comment == template.describe(order_items)
    assert list(definition.columns) == [
        column.lake_name for column in template.columns_for(order_items)
    ]
    assert definition.columns["quantity"].name == "quanity"
    assert definition.query == "SELECT 1"


def test_the_statement_describes_every_column_and_replaces_an_older_view() -> None:
    definition = ViewDefinition(
        comment="It's a view",
        columns={
            "id": ColumnDefinition("id", ColumnType.LONG, "Key"),
            "odd`name": ColumnDefinition("odd`name", ColumnType.STRING, "A \\ name"),
        },
        query="SELECT id, name\nFROM catalog.example.things",
    )

    assert create_view_statement("catalog.example.view", definition) == (
        "CREATE OR REPLACE VIEW catalog.example.view (\n"
        "  `id` COMMENT 'Key',\n"
        "  `odd``name` COMMENT 'A \\\\ name')\n"
        "COMMENT 'It\\'s a view'\n"
        "AS\n"
        "SELECT id, name\n"
        "FROM catalog.example.things"
    )


def test_a_missing_view_is_created_with_its_descriptions(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"

    assert ensure_view(spark, view, definition) == create_view_statement(view, definition)

    assert spark.table(view).collect() == [(2, 1)]
    stored = stored_description(spark, view)
    assert stored.comment == "The even numbers, it's \\ said"
    assert stored.columns == {"number": "An even number", "half": "Half of it, the number's half"}


def test_a_stored_view_reads_back_as_its_definition(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"
    ensure_view(spark, view, definition)

    assert stored_view(spark, view) == StoredView.of(definition)


def test_a_missing_view_has_no_stored_view_even_if_a_temporary_view_has_its_name(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    numbers_table(spark, iceberg_view_catalog)
    spark.range(1).createOrReplaceTempView("even_numbers")
    try:
        assert stored_view(spark, f"{iceberg_view_catalog}.example.even_numbers") is None
    finally:
        spark.catalog.dropTempView("even_numbers")


def test_an_unchanged_view_is_left_untouched(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"
    ensure_view(spark, view, definition)
    metadata_file = view_metadata_file(spark, iceberg_view_catalog, view)

    assert ensure_view(spark, view, definition) is None
    assert view_metadata_file(spark, iceberg_view_catalog, view) == metadata_file


def test_a_changed_description_replaces_the_view(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"
    ensure_view(spark, view, definition)
    metadata_file = view_metadata_file(spark, iceberg_view_catalog, view)
    number = replace(definition.columns["number"], description="Even")
    changed = definition._replace(columns={**definition.columns, "number": number})

    assert ensure_view(spark, view, changed) == create_view_statement(view, changed)

    assert view_metadata_file(spark, iceberg_view_catalog, view) != metadata_file
    assert stored_description(spark, view).columns["number"] == "Even"


def test_a_changed_comment_replaces_the_view(
    spark: SparkSession, iceberg_view_catalog: str
) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"
    ensure_view(spark, view, definition)
    changed = definition._replace(comment="Even numbers")

    assert ensure_view(spark, view, changed) == create_view_statement(view, changed)

    assert stored_description(spark, view).comment == "Even numbers"


def test_a_changed_query_replaces_the_view(spark: SparkSession, iceberg_view_catalog: str) -> None:
    definition = even_numbers(numbers_table(spark, iceberg_view_catalog))
    view = f"{iceberg_view_catalog}.example.even_numbers"
    ensure_view(spark, view, definition)
    changed = definition._replace(query=f"{definition.query} OR number = 3")

    assert ensure_view(spark, view, changed) == create_view_statement(view, changed)

    assert spark.table(view).orderBy("number").collect() == [(2, 1), (3, 1)]
