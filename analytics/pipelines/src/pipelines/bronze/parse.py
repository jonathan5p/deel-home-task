from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import NamedTuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pipelines.bronze.columns import MetadataColumn, RawColumn
from pipelines.bronze.content_hash import content_hash
from pipelines.bronze.decimals import decode_decimal, is_valid_decimal
from pipelines.bronze.entities import Entity
from pipelines.schemas.definitions import ColumnDefinition, ColumnType

CHANGE_OPS: Mapping[str, str] = {
    "c": "create",
    "u": "update",
    "d": "delete",
    "r": "snapshot",
}

_ENVELOPE = "_envelope"
_ROW = "_row"


class FailureReason(StrEnum):
    NOT_AN_ENVELOPE = "value is not a JSON Debezium envelope"
    UNKNOWN_OP = "unknown op"
    MISSING_KEY = "missing key column"
    MISSING_NOT_NULL = "missing NOT NULL column"
    INVALID_DECIMAL = "undecodable or overflowing decimal"


class ParsedChanges(NamedTuple):
    bronze: DataFrame
    quarantine: DataFrame


def _identity(value: Column) -> Column:
    return value


LANDING_TO_BRONZE_TYPE_CONVERSIONS: Mapping[ColumnType, Callable[[Column], Column]] = {
    ColumnType.LONG: _identity,
    ColumnType.INT: _identity,
    ColumnType.STRING: _identity,
    ColumnType.BOOLEAN: _identity,
    ColumnType.TIMESTAMP_MILLIS: F.timestamp_millis,
    ColumnType.DATE: F.date_from_unix_date,
    ColumnType.DECIMAL: decode_decimal,
}


def envelope_schema(entity: Entity) -> StructType:
    source = StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])
    return StructType(
        [
            StructField("before", entity.row_schema),
            StructField("after", entity.row_schema),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField("source", source),
        ]
    )


def _failure_checks(entity: Entity) -> list[tuple[Column, str]]:
    envelope = F.col(_ENVELOPE)
    row = F.col(_ROW)
    required = (
        envelope["op"],
        envelope["ts_ms"],
        envelope["source"]["lsn"],
        envelope["source"]["ts_ms"],
    )
    checks: list[tuple[Column, str]] = [
        (_any_null(required), FailureReason.NOT_AN_ENVELOPE),
        (~envelope["op"].isin(*CHANGE_OPS), FailureReason.UNKNOWN_OP),
        (row[entity.key].isNull(), f"{FailureReason.MISSING_KEY}: {entity.key}"),
    ]
    checks += [
        (row[name].isNull(), f"{FailureReason.MISSING_NOT_NULL}: {name}")
        for name in entity.not_null_columns
        if name != entity.key
    ]
    checks += [
        (~is_valid_decimal(row[column.name]), f"{FailureReason.INVALID_DECIMAL}: {column.name}")
        for column in entity.columns
        if column.type is ColumnType.DECIMAL
    ]
    return checks


def _any_null(columns: tuple[Column, ...]) -> Column:
    condition = columns[0].isNull()
    for column in columns[1:]:
        condition = condition | column.isNull()
    return condition


def failure_reason(entity: Entity) -> Column:
    """The first failed check of a parsed record, or null when it is valid."""
    (condition, reason), *rest = _failure_checks(entity)
    result = F.when(condition, F.lit(str(reason)))
    for condition, reason in rest:
        result = result.when(condition, F.lit(str(reason)))
    return result


def _checked(records: DataFrame, entity: Entity) -> DataFrame:
    envelope = F.from_json(F.col("value").cast(StringType()), envelope_schema(entity))
    # A delete carries the last known values in `before`; every other op in `after`.
    row = F.when(F.col(f"{_ENVELOPE}.op") == "d", F.col(f"{_ENVELOPE}.before")).otherwise(
        F.col(f"{_ENVELOPE}.after")
    )
    return (
        records.where(F.col("value").isNotNull())
        .withColumn(_ENVELOPE, envelope)
        .withColumn(_ROW, row)
        .withColumn(MetadataColumn.FAILURE_REASON, failure_reason(entity))
    )


def _change_op(op: Column) -> Column:
    (code, word), *rest = CHANGE_OPS.items()
    result = F.when(op == code, F.lit(word))
    for code, word in rest:
        result = result.when(op == code, F.lit(word))
    return result


def _business_column(column: ColumnDefinition) -> Column:
    convert = LANDING_TO_BRONZE_TYPE_CONVERSIONS[column.type]
    return convert(F.col(_ROW)[column.name]).alias(column.lake_name)


def _source_coordinates() -> list[Column]:
    return [
        F.col("topic").alias(MetadataColumn.RECORD_SOURCE),
        F.col("partition").alias(MetadataColumn.SOURCE_PARTITION),
        F.col("offset").alias(MetadataColumn.SOURCE_OFFSET),
        F.col("timestamp").alias(MetadataColumn.PUBLISHED_AT),
    ]


def _bronze(checked: DataFrame, entity: Entity) -> DataFrame:
    envelope = F.col(_ENVELOPE)
    typed = checked.where(F.col(MetadataColumn.FAILURE_REASON).isNull()).select(
        *(_business_column(column) for column in entity.columns),
        *_source_coordinates(),
        _change_op(envelope["op"]).alias(MetadataColumn.CHANGE_OP),
        envelope["source"]["lsn"].alias(MetadataColumn.COMMIT_SEQUENCE),
        F.timestamp_millis(envelope["source"]["ts_ms"]).alias(MetadataColumn.CHANGED_AT),
        F.timestamp_millis(envelope["ts_ms"]).alias(MetadataColumn.CAPTURED_AT),
    )
    return typed.withColumn(
        MetadataColumn.CONTENT_HASH, content_hash(typed.schema, entity.content_columns)
    )


def _quarantine(checked: DataFrame) -> DataFrame:
    return checked.where(F.col(MetadataColumn.FAILURE_REASON).isNotNull()).select(
        *_source_coordinates(),
        F.col(MetadataColumn.LANDING_FILE),
        F.col("key").cast(StringType()).alias(RawColumn.KEY),
        F.col("value").cast(StringType()).alias(RawColumn.VALUE),
        F.col(MetadataColumn.FAILURE_REASON),
    )


def parse_changes(records: DataFrame, entity: Entity) -> ParsedChanges:
    """Split landing records of one entity into typed bronze rows and quarantine rows.

    `records` holds the Kafka columns as landed plus `_landing_file`, the path of the landing
    file each record was read from; quarantine rows keep it so a failure can be traced back
    to its file. Tombstones (null values) are dropped from both.
    """
    checked = _checked(records, entity)
    return ParsedChanges(bronze=_bronze(checked, entity), quarantine=_quarantine(checked))
