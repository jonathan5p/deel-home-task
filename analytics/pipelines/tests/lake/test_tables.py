import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from analytics.pipelines.tests.lake.tables_setup import (
    DEFINITION,
    SCHEMA,
    TABLE,
    metadata_versions,
    redescribed,
    stored_description,
    table_ddl,
)
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.tables import (
    TABLE_TEMPLATES,
    ColumnTypeMismatchError,
    UndescribedColumnError,
    described_schema,
    description_updates,
    ensure_table,
    sql_identifier,
    sql_string,
    table_definition,
)
from pipelines.schemas.definitions import load_entity


def test_a_table_definition_takes_the_template_description_and_columns_by_lake_name() -> None:
    order_items = load_entity("order_items")

    definition = table_definition("bronze", order_items, partitioned_by=MetadataColumn.PUBLISHED_AT)

    template = TABLE_TEMPLATES["bronze"]
    assert definition.comment == template.describe(order_items)
    assert list(definition.columns) == [
        column.lake_name for column in template.columns_for(order_items)
    ]
    assert definition.columns["quantity"].name == "quanity"
    assert definition.partitioned_by == MetadataColumn.PUBLISHED_AT


def test_the_described_schema_carries_each_description_as_the_field_comment() -> None:
    schema = described_schema(TABLE, SCHEMA, DEFINITION)

    assert [(field.name, field.dataType) for field in schema] == [
        (field.name, field.dataType) for field in SCHEMA
    ]
    assert [field.metadata for field in schema] == [
        {"comment": "Key"},
        {"comment": "Price"},
        {"comment": "Published"},
    ]
    assert all(field.nullable for field in schema)


def test_a_column_without_a_definition_is_refused() -> None:
    columns = {name: column for name, column in DEFINITION.columns.items() if name != "unity_price"}

    with pytest.raises(UndescribedColumnError, match=r"customers have no definition: unity_price$"):
        described_schema(TABLE, SCHEMA, DEFINITION._replace(columns=columns))


def test_a_column_whose_type_differs_from_its_definition_is_refused() -> None:
    schema = StructType([StructField("customer_id", IntegerType()), *SCHEMA.fields[1:]])

    with pytest.raises(ColumnTypeMismatchError, match=r"customer_id is int, defined as long$"):
        described_schema(TABLE, schema, DEFINITION)


@pytest.mark.parametrize(
    ("text", "literal"),
    [
        ("plain", "'plain'"),
        ("the customer's name", "'the customer\\'s name'"),
        ("a \\ path", "'a \\\\ path'"),
    ],
)
def test_sql_strings_escape_quotes_and_backslashes(text: str, literal: str) -> None:
    assert sql_string(text) == literal


def test_sql_identifiers_are_quoted() -> None:
    assert sql_identifier("odd`name") == "`odd``name`"


def test_matching_descriptions_need_no_update() -> None:
    schema = described_schema(TABLE, SCHEMA, DEFINITION)

    assert description_updates(TABLE, schema, "Customers", schema, "Customers") == []


def test_only_changed_descriptions_are_updated() -> None:
    stored = described_schema(TABLE, SCHEMA, DEFINITION)
    schema = described_schema(TABLE, SCHEMA, redescribed(DEFINITION, unity_price="It's a price"))

    assert description_updates(TABLE, stored, None, schema, "Customers") == [
        f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('comment' = 'Customers')",
        f"ALTER TABLE {TABLE} ALTER COLUMN `unity_price` COMMENT 'It\\'s a price'",
    ]


def test_columns_missing_on_either_side_are_left_to_schema_changes() -> None:
    stored = described_schema(TABLE, SCHEMA, DEFINITION)
    schema = StructType(
        [
            *stored.fields[:2],
            StructField("added", StringType(), metadata={"comment": "Not in the table yet"}),
        ]
    )

    assert description_updates(TABLE, stored, "Customers", schema, "Customers") == []


def test_a_new_table_is_created_with_its_descriptions_partitioned_by_day(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)

    assert ensure_table(spark, table, schema, DEFINITION._replace(comment="It's a \\ table")) == []

    stored = stored_description(spark, table)
    assert stored.comment == "It's a \\ table"
    assert stored.columns == {
        "customer_id": "Key",
        "unity_price": "Price",
        "_published_at": "Published",
    }
    ddl = table_ddl(spark, table)
    assert f"PARTITIONED BY (days({DEFINITION.partitioned_by}))" in ddl
    assert "'write.distribution-mode' = 'none'" in ddl


def test_an_existing_table_gets_only_its_changed_descriptions(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    ensure_table(spark, table, described_schema(table, SCHEMA, DEFINITION), DEFINITION)
    versions = metadata_versions(spark, table)
    changed = redescribed(DEFINITION, unity_price="The customer's \\ price")

    updates = ensure_table(spark, table, described_schema(table, SCHEMA, changed), changed)

    assert updates == [
        f"ALTER TABLE {table} ALTER COLUMN `unity_price` COMMENT 'The customer\\'s \\\\ price'"
    ]
    assert stored_description(spark, table).columns["unity_price"] == "The customer's \\ price"
    assert stored_description(spark, table).columns["customer_id"] == "Key"
    assert metadata_versions(spark, table) == versions + 1


def test_an_unchanged_table_is_left_untouched(spark: SparkSession, iceberg_catalog: str) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)
    ensure_table(spark, table, schema, DEFINITION)
    versions = metadata_versions(spark, table)

    assert ensure_table(spark, table, schema, DEFINITION) == []
    assert metadata_versions(spark, table) == versions


def test_a_changed_table_comment_is_updated(spark: SparkSession, iceberg_catalog: str) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)
    ensure_table(spark, table, schema, DEFINITION)

    ensure_table(spark, table, schema, DEFINITION._replace(comment="Customers who order"))

    assert stored_description(spark, table).comment == "Customers who order"
