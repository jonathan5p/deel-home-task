from collections.abc import Iterable, Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipelines.lake.gold.facts import AsOfLookup, KeyLookup
from pipelines.lake.gold.model.versions import (
    CHANGED_AT,
    COMMIT_SEQUENCE,
    IS_DELETED,
    VALID_FROM,
    VersionColumn,
)
from pipelines.lake.shared.types import LAKE_DECIMAL

FACTOR_DECIMAL = DecimalType(27, 10)


def with_key_lookup(rows: DataFrame, lookup: KeyLookup, seeded: DataFrame) -> DataFrame:
    """`rows` with `lookup.column`: the `key` of the seeded dimension row whose `code` equals the
    row's `source`, or of the `fallback` code's row when none does. The seeded dimensions are
    small, so both joins are broadcast."""
    code, fallback = "__lookup_code", "__lookup_fallback"
    matches = seeded.select(F.col(lookup.code).alias(code), F.col(lookup.key).alias(lookup.column))
    looked_up = rows.drop(lookup.column).join(
        F.broadcast(matches), F.col(lookup.source) == F.col(code), "left"
    )
    if lookup.fallback is None:
        return looked_up.drop(code)
    default = seeded.where(F.col(lookup.code) == lookup.fallback).select(
        F.col(lookup.key).alias(fallback)
    )
    return (
        looked_up.join(F.broadcast(default), F.lit(True), "left")
        .withColumn(lookup.column, F.coalesce(F.col(lookup.column), F.col(fallback)))
        .drop(code, fallback)
    )


def with_key_lookups(
    rows: DataFrame, lookups: Iterable[KeyLookup], seeded: Mapping[str, DataFrame]
) -> DataFrame:
    """`rows` with every lookup's column; `seeded` holds each seeded dimension by table name."""
    for lookup in lookups:
        rows = with_key_lookup(rows, lookup, seeded[lookup.dimension.name])
    return rows


def with_as_of_lookup(
    rows: DataFrame, key: str, lookup: AsOfLookup, versions: DataFrame
) -> DataFrame:
    """`rows` with the columns `lookup` takes from the dimension version valid when each change
    was committed, replacing any values they had.

    The version is the one with the highest `_commit_sequence` among those not folded, not
    deleted, and with the change's `_changed_at` in [`_valid_from`, `_valid_to`). The first
    version also covers every earlier time, because the source snapshots each table at a
    slightly different time. The columns are null when the key has no version yet or the
    version at that time is a delete.

    `rows` are identified by `key` and `_commit_sequence`; `versions` holds the dimension's
    stored rows with their version columns, at least those of the rows' business keys. A
    multiplied column is the factor times the looked-up column, at the lake's decimal type.
    """
    looked_up = [fact for fact, _ in lookup.columns]
    candidates = versions.where(~F.col(VersionColumn.IS_FOLDED) & ~F.col(IS_DELETED)).select(
        F.col(lookup.dimension.key).alias("__version_key"),
        F.col(COMMIT_SEQUENCE).alias("__version_sequence"),
        F.col(VALID_FROM).alias("__version_from"),
        F.col(VersionColumn.VALID_TO).alias("__version_to"),
        (F.col(VersionColumn.VERSION_NUMBER) == 1).alias("__version_first"),
        *(F.col(column).alias(fact) for fact, column in lookup.columns),
    )
    base = rows.drop(*lookup.fact_columns)
    changes = base.select(key, COMMIT_SEQUENCE, CHANGED_AT, lookup.business_key)
    valid = (
        (F.col(lookup.business_key) == F.col("__version_key"))
        & ((F.col("__version_from") <= F.col(CHANGED_AT)) | F.col("__version_first"))
        & (F.col("__version_to").isNull() | (F.col(CHANGED_AT) < F.col("__version_to")))
    )
    latest = Window.partitionBy(key, COMMIT_SEQUENCE).orderBy(F.col("__version_sequence").desc())
    matched = (
        changes.join(candidates, valid)
        .withColumn("__pick", F.row_number().over(latest))
        .where(F.col("__pick") == 1)
        .select(key, COMMIT_SEQUENCE, *looked_up)
    )
    resolved = base.join(matched, [key, COMMIT_SEQUENCE], "left")
    for multiplied in lookup.multiplied:
        amount = F.col(multiplied.looked_up).cast(FACTOR_DECIMAL) * F.col(multiplied.factor)
        resolved = resolved.withColumn(multiplied.column, amount.cast(LAKE_DECIMAL))
    return resolved
