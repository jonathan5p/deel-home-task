from collections.abc import Mapping

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from pipelines.schemas.definitions import ColumnType

LAKE_DECIMAL = DecimalType(38, 10)
SPARK_TYPES: Mapping[ColumnType, DataType] = {
    ColumnType.LONG: LongType(),
    ColumnType.INT: IntegerType(),
    ColumnType.STRING: StringType(),
    ColumnType.BOOLEAN: BooleanType(),
    ColumnType.TIMESTAMP_MILLIS: TimestampType(),
    ColumnType.DATE: DateType(),
    ColumnType.DECIMAL: LAKE_DECIMAL,
}
