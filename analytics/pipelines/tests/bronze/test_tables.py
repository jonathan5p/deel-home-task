import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from analytics.pipelines.tests.bronze.tables_setup import (
    DEFINITION,
    SCHEMA,
    TABLE,
    metadata_versions,
    redescribed,
    stored_description,
)
from common.definitions import load_columns
from pipelines.bronze.columns import MetadataColumn, RawColumn
from pipelines.bronze.entities import ORDER_ITEMS
from pipelines.bronze.tables import (
    ColumnTypeMismatchError,
    UndescribedColumnError,
    bronze_table,
    described_schema,
    description_updates,
    ensure_table,
    quarantine_table,
    sql_identifier,
    sql_string,
)


def test_bronze_tables_describe_entity_columns_by_their_lake_name() -> None:
    columns = bronze_table(ORDER_ITEMS).columns

    assert columns["quantity"].description == "Number of units ordered (source column quanity)"
    assert "quanity" not in columns
    assert columns[MetadataColumn.COMMIT_SEQUENCE] == load_columns()["_commit_sequence"]


def test_table_comments_come_from_the_templates_and_name_the_source_table() -> None:
    bronze = bronze_table(ORDER_ITEMS).comment
    quarantine = quarantine_table(ORDER_ITEMS).comment

    assert bronze.startswith(f"{ORDER_ITEMS.definition.description}. Change log of source table")
    assert "source table order_items" in quarantine


def test_quarantine_tables_describe_the_raw_kafka_columns() -> None:
    columns = quarantine_table(ORDER_ITEMS).columns

    assert columns[RawColumn.KEY].description == "Raw Kafka record key, as text"
    assert "quantity" not in columns


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

    with pytest.raises(ColumnTypeMismatchError, match=r"customer_id is int, defined as bigint$"):
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


def test_a_new_table_is_created_with_its_descriptions_partitioned_by_publish_day(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)

    assert ensure_table(spark, table, schema, "It's a \\ table") == []

    stored = stored_description(spark, table)
    assert stored.comment == "It's a \\ table"
    assert stored.columns == {
        "customer_id": "Key",
        "unity_price": "Price",
        "_published_at": "Published",
    }
    ddl = spark.sql(f"SHOW CREATE TABLE {table}").first()
    assert ddl is not None
    assert f"PARTITIONED BY (days({MetadataColumn.PUBLISHED_AT}))" in ddl[0]
    assert "'write.distribution-mode' = 'none'" in ddl[0]


def test_an_existing_table_gets_only_its_changed_descriptions(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    ensure_table(spark, table, described_schema(table, SCHEMA, DEFINITION), "Customers")
    versions = metadata_versions(spark, table)
    changed = redescribed(DEFINITION, unity_price="The customer's \\ price")

    updates = ensure_table(spark, table, described_schema(table, SCHEMA, changed), "Customers")

    assert updates == [
        f"ALTER TABLE {table} ALTER COLUMN `unity_price` COMMENT 'The customer\\'s \\\\ price'"
    ]
    assert stored_description(spark, table).columns["unity_price"] == "The customer's \\ price"
    assert stored_description(spark, table).columns["customer_id"] == "Key"
    assert metadata_versions(spark, table) == versions + 1


def test_an_unchanged_table_is_left_untouched(spark: SparkSession, iceberg_catalog: str) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)
    ensure_table(spark, table, schema, "Customers")
    versions = metadata_versions(spark, table)

    assert ensure_table(spark, table, schema, "Customers") == []
    assert metadata_versions(spark, table) == versions


def test_a_changed_table_comment_is_updated(spark: SparkSession, iceberg_catalog: str) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    schema = described_schema(table, SCHEMA, DEFINITION)
    ensure_table(spark, table, schema, "Customers")

    ensure_table(spark, table, schema, "Customers who order")

    assert stored_description(spark, table).comment == "Customers who order"
