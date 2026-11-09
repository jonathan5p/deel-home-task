from collections.abc import Iterable

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

DELIMITER = "|"
DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
PLAIN_TYPES = (StringType, LongType, IntegerType, BooleanType, DecimalType)


class UnsupportedHashTypeError(TypeError):
    def __init__(self, column: str, data_type: DataType) -> None:
        super().__init__(f"Column {column!r} of type {data_type.simpleString()} cannot be hashed")


def _utc_iso(timestamp: Column) -> Column:
    # date_format renders in the session time zone; going through the UTC wall clock makes
    # the text, and therefore the hash, independent of that setting.
    utc = F.convert_timezone(None, F.lit("UTC"), timestamp.cast(TimestampNTZType()))
    return F.date_format(utc, TIMESTAMP_FORMAT)


def _formatted(column: str, data_type: DataType) -> Column:
    value = F.col(column)
    if isinstance(data_type, TimestampType):
        return _utc_iso(value)
    if isinstance(data_type, DateType):
        return F.date_format(value, DATE_FORMAT)
    if isinstance(data_type, PLAIN_TYPES):
        return value.cast(StringType())
    raise UnsupportedHashTypeError(column, data_type)


def _part(column: str, data_type: DataType) -> Column:
    text = _formatted(column, data_type)
    return F.when(
        F.col(column).isNotNull(),
        F.concat(F.lit(f"{column}:"), F.length(text).cast(StringType()), F.lit(":"), text),
    )


def content_hash(schema: StructType, columns: Iterable[str]) -> Column:
    """SHA-256 of `name:length:value` for each non-null column, sorted by name.

    Null columns are left out, so a new column that is still null changes no hash, while an
    empty string still contributes `name:0:`.
    """
    parts = [_part(column, schema[column].dataType) for column in sorted(columns)]
    return F.sha2(F.concat_ws(DELIMITER, *parts), 256)
