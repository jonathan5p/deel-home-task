from dataclasses import replace
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, LongType, StructField, StructType, TimestampType

from pipelines.bronze.columns import MetadataColumn
from pipelines.bronze.tables import TableDefinition, table_comment
from pipelines.schemas.definitions import ColumnDefinition, ColumnType

TABLE = "catalog.bronze.customers"
SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), nullable=False),
        StructField("unity_price", DecimalType(38, 10)),
        StructField(MetadataColumn.PUBLISHED_AT, TimestampType()),
    ]
)
DEFINITION = TableDefinition(
    comment="Customers",
    columns={
        "customer_id": ColumnDefinition("customer_id", ColumnType.LONG, "Key"),
        "unity_price": ColumnDefinition("unity_price", ColumnType.DECIMAL, "Price"),
        MetadataColumn.PUBLISHED_AT: ColumnDefinition(
            MetadataColumn.PUBLISHED_AT, ColumnType.TIMESTAMP_MILLIS, "Published"
        ),
    },
)


def redescribed(definition: TableDefinition, **descriptions: str) -> TableDefinition:
    """`definition` with new descriptions for the named columns."""
    columns = {
        name: replace(column, description=descriptions[name]) if name in descriptions else column
        for name, column in definition.columns.items()
    }
    return definition._replace(columns=columns)


class StoredDescription(NamedTuple):
    comment: str | None
    columns: dict[str, str | None]


def stored_description(spark: SparkSession, table: str) -> StoredDescription:
    """The table comment and column docs an Iceberg table carries in its metadata."""
    columns = {field.name: field.metadata.get("comment") for field in spark.table(table).schema}
    return StoredDescription(comment=table_comment(spark, table), columns=columns)


def metadata_versions(spark: SparkSession, table: str) -> int:
    """How many metadata files the table has written; every commit or ALTER adds one."""
    return spark.table(f"{table}.metadata_log_entries").count()
