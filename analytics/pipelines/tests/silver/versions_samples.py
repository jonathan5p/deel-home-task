from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from pipelines.bronze.entities import ENTITIES, Entity
from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.definitions import ColumnDefinition, ColumnType

ENTITY_IDS = [entity.name for entity in ENTITIES]
CHANGED_AT = datetime(2026, 11, 8, 14, 21)  # Spark returns naive timestamps in UTC

type BronzeChange = dict[str, object]

SAMPLE_VALUES: Mapping[ColumnType, Callable[[int], object]] = {
    ColumnType.LONG: lambda sequence: sequence,
    ColumnType.INT: lambda sequence: sequence,
    ColumnType.STRING: lambda sequence: f"value {sequence}",
    ColumnType.BOOLEAN: lambda sequence: sequence % 2 == 0,
    ColumnType.TIMESTAMP_MILLIS: lambda sequence: CHANGED_AT + timedelta(seconds=sequence),
    ColumnType.DATE: lambda sequence: date(2026, 11, 8) + timedelta(days=sequence),
    ColumnType.DECIMAL: lambda sequence: Decimal(sequence).scaleb(-2),
}


def bronze_columns(entity: Entity) -> tuple[ColumnDefinition, ...]:
    return TABLE_TEMPLATES["bronze"].columns_for(entity.definition)


def bronze_schema(entity: Entity) -> StructType:
    """The columns and types of the entity's bronze table."""
    return StructType(
        [
            StructField(column.lake_name, SPARK_TYPES[column.type])
            for column in bronze_columns(entity)
        ]
    )


def bronze_change(entity: Entity, sequence: int, op: ChangeOp) -> BronzeChange:
    """A bronze row of `entity` committed at `sequence`, with every other value derived from it."""
    values = {
        column.lake_name: SAMPLE_VALUES[column.type](sequence) for column in bronze_columns(entity)
    }
    return values | {MetadataColumn.CHANGE_OP: str(op), MetadataColumn.COMMIT_SEQUENCE: sequence}


def bronze_frame(spark: SparkSession, entity: Entity, changes: list[BronzeChange]) -> DataFrame:
    schema = bronze_schema(entity)
    rows = [tuple(change[name] for name in schema.fieldNames()) for change in changes]
    return spark.createDataFrame(rows, schema)
