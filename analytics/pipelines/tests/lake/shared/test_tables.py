import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from analytics.pipelines.tests.lake.shared.tables_setup import (
    DEFINITION,
    SCHEMA,
    TABLE,
    metadata_versions,
    redescribed,
    stored_description,
    table_ddl,
)
from pipelines.lake.bronze.columns import RawColumn
from pipelines.lake.bronze.entities import ORDER_ITEMS
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.tables import (
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
from pipelines.lake.silver.columns import SILVER_METADATA_COLUMNS
from pipelines.schemas.definitions import load_entity


def test_a_table_definition_takes_the_template_description_and_columns_by_lake_name() -> None:
    order_items = load_entity("order_items")

    definition = table_definition("bronze", order_items)

    template = TABLE_TEMPLATES["bronze"]
    assert definition.comment == template.describe(order_items)
    assert list(definition.columns) == [
        column.lake_name for column in template.columns_for(order_items)
    ]
    assert definition.columns["quantity"].name == "quanity"
    assert definition.partitioned_by == template.partitioned_by


def test_bronze_tables_describe_entity_columns_by_their_lake_name() -> None:
    columns = table_definition("bronze", ORDER_ITEMS.definition).columns

    assert columns["quantity"].description == "Number of units ordered (source column quanity)"
    assert "quanity" not in columns
    assert columns[MetadataColumn.COMMIT_SEQUENCE] in TABLE_TEMPLATES["bronze"].fields
    assert MetadataColumn.LANDING_FILE not in columns
    assert RawColumn.KEY not in columns


def test_quarantine_tables_describe_the_raw_kafka_columns() -> None:
    columns = table_definition("bronze_quarantine", ORDER_ITEMS.definition).columns

    assert columns[RawColumn.KEY].description == "Raw Kafka record key, as text"
    assert "quantity" not in columns
    assert MetadataColumn.COMMIT_SEQUENCE not in columns


def test_silver_tables_describe_the_entity_columns_then_the_version_columns() -> None:
    columns = table_definition("silver", ORDER_ITEMS.definition).columns

    assert list(columns) == [
        *(column.lake_name for column in ORDER_ITEMS.columns),
        *SILVER_METADATA_COLUMNS,
    ]


def test_table_comments_come_from_the_templates_and_name_the_source_table() -> None:
    description = ORDER_ITEMS.definition.description

    def comment(template: str) -> str:
        return table_definition(template, ORDER_ITEMS.definition).comment

    assert comment("bronze").startswith(f"{description}. Change log of source table")
    assert "source table order_items" in comment("bronze_quarantine")
    assert comment("silver").startswith(
        f"{description}. Append-only version history of source table order_items"
    )


@pytest.mark.parametrize(
    ("template", "column"),
    [
        ("bronze", MetadataColumn.PUBLISHED_AT),
        ("bronze_quarantine", MetadataColumn.PUBLISHED_AT),
        ("silver", MetadataColumn.CHANGED_AT),
    ],
)
def test_each_layer_table_is_partitioned_by_the_day_of_its_template_column(
    template: str, column: str
) -> None:
    assert table_definition(template, ORDER_ITEMS.definition).partitioned_by == column


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


def test_a_table_without_a_partition_column_is_created_unpartitioned(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.unpartitioned"
    schema = described_schema(table, SCHEMA, DEFINITION)

    ensure_table(spark, table, schema, DEFINITION._replace(partitioned_by=None))

    assert "PARTITIONED BY" not in table_ddl(spark, table)


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
