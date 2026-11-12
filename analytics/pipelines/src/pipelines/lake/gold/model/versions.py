from collections.abc import Sequence
from enum import StrEnum

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pipelines.lake.shared.columns import MetadataColumn

COMMIT_SEQUENCE = str(MetadataColumn.COMMIT_SEQUENCE)
CONTENT_HASH = str(MetadataColumn.CONTENT_HASH)
IS_DELETED = str(MetadataColumn.IS_DELETED)
CHANGED_AT = str(MetadataColumn.CHANGED_AT)
VALID_FROM = str(MetadataColumn.VALID_FROM)


class VersionColumn(StrEnum):
    IS_FOLDED = "_is_folded"
    VERSION_NUMBER = "_version_number"
    VALID_TO = "_valid_to"
    IS_CURRENT = "_is_current"


VERSION_COLUMNS = tuple(str(column) for column in VersionColumn)


def all_of(conditions: Sequence[Column]) -> Column:
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = combined & condition
    return combined


def any_of(conditions: Sequence[Column]) -> Column:
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = combined | condition
    return combined


def new_changes(batch: DataFrame, stored: DataFrame, key: str) -> DataFrame:
    """The changes of `batch` not stored yet, one per `key` and `_commit_sequence`. A replayed
    change is skipped, so the stored row, with the values resolved when it was written, wins."""
    identity = [key, COMMIT_SEQUENCE]
    pick = Window.partitionBy(*identity).orderBy(CONTENT_HASH, IS_DELETED)
    distinct = (
        batch.withColumn("__pick", F.row_number().over(pick))
        .where(F.col("__pick") == 1)
        .drop("__pick")
    )
    return distinct.join(stored.select(*identity), identity, "left_anti").select(*batch.columns)


def with_versions(rows: DataFrame, key: str, starts_at: str) -> DataFrame:
    """`rows`, holding every change of their keys, with the columns that place each change
    among its key's changes in `_commit_sequence` order.

    A change is folded when its content hash and deletion state equal the previous change's;
    the first change is never folded. A change that is not folded starts a new version number
    and a folded one shares the number of the change it repeats, so it ends when that version
    ends: `_valid_to` is the `starts_at` of the first change of the next number. The last number
    is current unless it is a delete, and a folded change never is.

    Every column is derived from all the changes of the key, so a late change re-places the
    changes after it: one landing between two equal changes unfolds the later one.
    """
    by_key = Window.partitionBy(key).orderBy(COMMIT_SEQUENCE)
    folded = F.col(CONTENT_HASH).eqNullSafe(F.lag(CONTENT_HASH).over(by_key)) & F.col(
        IS_DELETED
    ).eqNullSafe(F.lag(IS_DELETED).over(by_key))
    running = by_key.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    starts_version = F.when(~F.col(VersionColumn.IS_FOLDED), 1).otherwise(0)
    next_number = Window.partitionBy(key).orderBy(VersionColumn.VERSION_NUMBER).rangeBetween(1, 1)
    next_start = F.when(~F.col(VersionColumn.IS_FOLDED), F.col(starts_at))
    last_number = F.max(VersionColumn.VERSION_NUMBER).over(Window.partitionBy(key))
    return (
        rows.withColumn(VersionColumn.IS_FOLDED, folded)
        .withColumn(VersionColumn.VERSION_NUMBER, F.sum(starts_version).over(running))
        .withColumn(VersionColumn.VALID_TO, F.max(next_start).over(next_number))
        .withColumn(
            VersionColumn.IS_CURRENT,
            ~F.col(VersionColumn.IS_FOLDED)
            & ~F.col(IS_DELETED)
            & (F.col(VersionColumn.VERSION_NUMBER) == last_number),
        )
    )


def changed_rows(
    recomputed: DataFrame, stored: DataFrame, keys: Sequence[str], compared: Sequence[str]
) -> DataFrame:
    """The rows of `recomputed` that `stored` has no row for, by `keys`, or whose `compared`
    values differ from the stored row's, nulls comparing equal. A replayed batch that changes
    nothing gives no rows."""
    before = stored.select(
        *keys,
        *(F.col(column).alias(f"__stored_{column}") for column in compared),
        F.lit(True).alias("__stored"),
    )
    differs = any_of(
        [~F.col(column).eqNullSafe(F.col(f"__stored_{column}")) for column in compared]
    )
    return (
        recomputed.join(before, list(keys), "left")
        .where(F.col("__stored").isNull() | differs)
        .select(*recomputed.columns)
    )


def version_changes(recomputed: DataFrame, stored: DataFrame, key: str) -> DataFrame:
    """The recomputed changes to write: the new ones, and the stored ones whose version columns
    changed.

    The writer merges them on `key` and `_commit_sequence`: when matched, update the version
    columns; when not matched, insert the row.
    """
    return changed_rows(recomputed, stored, (key, COMMIT_SEQUENCE), VERSION_COLUMNS)
