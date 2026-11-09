from collections.abc import Iterable, Mapping
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import partitioning
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.definitions import ColumnDefinition, LogicalType, load_columns, load_table_templates
from pipelines.bronze.columns import MetadataColumn
from pipelines.bronze.decimals import BRONZE_DECIMAL
from pipelines.bronze.entities import Entity

SPARK_TYPES: Mapping[LogicalType, DataType] = {
    LogicalType.BIGINT: LongType(),
    LogicalType.INTEGER: IntegerType(),
    LogicalType.VARCHAR: StringType(),
    LogicalType.BOOLEAN: BooleanType(),
    LogicalType.TIMESTAMP: TimestampType(),
    LogicalType.DATE: DateType(),
    LogicalType.DECIMAL: BRONZE_DECIMAL,
}
PIPELINE_COLUMNS = load_columns()
TABLE_TEMPLATES = load_table_templates()
COMMENT = "comment"


class TableDefinition(NamedTuple):
    comment: str
    columns: Mapping[str, ColumnDefinition]


class UndescribedColumnError(ValueError):
    def __init__(self, table: str, columns: Iterable[str]) -> None:
        super().__init__(f"Columns of {table} have no definition: {', '.join(columns)}")


class ColumnTypeMismatchError(TypeError):
    def __init__(self, table: str, mismatches: Iterable[str]) -> None:
        super().__init__(
            f"Columns of {table} differ from their definition: {'; '.join(mismatches)}"
        )


def bronze_table(entity: Entity) -> TableDefinition:
    """The pipeline columns plus the entity's columns, under their lake names."""
    own = {column.lake_name: column for column in entity.columns}
    return TableDefinition(
        comment=TABLE_TEMPLATES["bronze"].describe(entity.definition),
        columns={**PIPELINE_COLUMNS, **own},
    )


def quarantine_table(entity: Entity) -> TableDefinition:
    return TableDefinition(
        comment=TABLE_TEMPLATES["bronze_quarantine"].describe(entity.definition),
        columns=PIPELINE_COLUMNS,
    )


def described_schema(table: str, schema: StructType, definition: TableDefinition) -> StructType:
    """`schema` with every field's description as its comment, checked against its definition.

    Every field is nullable, as in a table created by `CREATE TABLE` without `NOT NULL`, so a
    column's nullability can change later without rewriting the table.
    """
    undescribed = [field.name for field in schema if field.name not in definition.columns]
    if undescribed:
        raise UndescribedColumnError(table, undescribed)
    defined = {field.name: definition.columns[field.name].type for field in schema}
    mismatches = [
        f"{field.name} is {field.dataType.simpleString()}, defined as {defined[field.name]}"
        for field in schema
        if SPARK_TYPES[defined[field.name]] != field.dataType
    ]
    if mismatches:
        raise ColumnTypeMismatchError(table, mismatches)
    return StructType(
        [
            StructField(
                field.name,
                field.dataType,
                nullable=True,
                metadata={COMMENT: definition.columns[field.name].description},
            )
            for field in schema
        ]
    )


def sql_string(text: str) -> str:
    """A Spark SQL string literal that reads back as `text`."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def sql_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def description_updates(
    table: str,
    stored: StructType,
    stored_comment: str | None,
    schema: StructType,
    comment: str,
) -> list[str]:
    """The statements that bring a table's stored descriptions in line with `schema`.

    Only differing descriptions get a statement; columns missing on either side are left alone,
    because schema changes are applied by hand.
    """
    updates = []
    if stored_comment != comment:
        updates.append(f"ALTER TABLE {table} SET TBLPROPERTIES ('comment' = {sql_string(comment)})")
    stored_names = set(stored.fieldNames())
    for field in schema:
        description = field.metadata[COMMENT]
        if field.name in stored_names and stored[field.name].metadata.get(COMMENT) != description:
            updates.append(
                f"ALTER TABLE {table} ALTER COLUMN {sql_identifier(field.name)}"
                f" COMMENT {sql_string(description)}"
            )
    return updates


def table_comment(spark: SparkSession, table: str) -> str | None:
    # SHOW TBLPROPERTIES hides the reserved `comment` property; DESCRIBE lists it as a row.
    details = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    return next((row.data_type for row in details if row.col_name == "Comment"), None)


def create_table(spark: SparkSession, table: str, schema: StructType, comment: str) -> None:
    namespace, _, _ = table.rpartition(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    # Streaming disables adaptive execution, so the default hash distribution would shuffle
    # every micro-batch into spark.sql.shuffle.partitions tasks; a local sort is enough here.
    (
        spark.createDataFrame([], schema)
        .writeTo(table)
        .using("iceberg")
        .partitionedBy(partitioning.days(MetadataColumn.PUBLISHED_AT))
        .tableProperty("write.distribution-mode", "none")
        .tableProperty(COMMENT, comment)
        .create()
    )


def ensure_table(spark: SparkSession, table: str, schema: StructType, comment: str) -> list[str]:
    """Create `table` with its descriptions, or update the descriptions of an existing one.

    Returns the description updates applied to an existing table.
    """
    if not spark.catalog.tableExists(table):
        create_table(spark, table, schema, comment)
        return []
    stored = spark.table(table).schema
    updates = description_updates(table, stored, table_comment(spark, table), schema, comment)
    for statement in updates:
        spark.sql(statement)
    return updates
