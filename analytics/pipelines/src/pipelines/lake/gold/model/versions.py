from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pipelines.lake.gold.model.columns import VERSION_COLUMNS, VersionColumn
from pipelines.lake.shared.columns import MetadataColumn

COMMIT_SEQUENCE = str(MetadataColumn.COMMIT_SEQUENCE)
CONTENT_HASH = str(MetadataColumn.CONTENT_HASH)
IS_DELETED = str(MetadataColumn.IS_DELETED)
CHANGED_AT = str(MetadataColumn.CHANGED_AT)
VALID_FROM = str(MetadataColumn.VALID_FROM)


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


def touched(stored: DataFrame, batch: DataFrame, key: str) -> DataFrame:
    """The `stored` rows of the keys `batch` touches. A batch holds few keys, so they are
    broadcast and the stored rows are filtered where they are read, with no shuffle."""
    return stored.join(F.broadcast(batch.select(key)), key, "left_semi")


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


def version_changes(stored: DataFrame, batch: DataFrame, key: str, starts_at: str) -> DataFrame:
    """The changes to write once `batch` joins the `stored` changes of its keys: its changes not
    stored yet, and the stored ones whose version columns it changes, each with the version
    columns recomputed from every change of its key (`with_versions`).

    `stored` holds the stored rows of the keys `batch` touches, with their version columns;
    `batch` holds its changes with the other columns. A change already stored, or delivered
    twice, counts once: the stored row wins, then the lowest content hash. Every row is placed
    and compared with its stored self in one pass over the rows partitioned by `key`, so a
    replayed batch that changes nothing gives no rows.

    The writer merges them on `key` and `_commit_sequence`: when matched, update the version
    columns; when not matched, insert the row.
    """
    columns = batch.columns
    previous = stored.select(
        *columns,
        *(F.col(column).alias(f"__stored_{column}") for column in VERSION_COLUMNS),
        F.lit(True).alias("__stored"),
    )
    incoming = batch.withColumn("__stored", F.lit(False))
    rows = previous.unionByName(incoming, allowMissingColumns=True)
    by_change = Window.partitionBy(key).orderBy(
        COMMIT_SEQUENCE, F.col("__stored").desc(), CONTENT_HASH, IS_DELETED
    )
    repeated = F.lag(COMMIT_SEQUENCE).over(by_change).eqNullSafe(F.col(COMMIT_SEQUENCE))
    changes = rows.withColumn("__repeat", repeated).where(~F.col("__repeat")).drop("__repeat")
    differs = any_of(
        [~F.col(column).eqNullSafe(F.col(f"__stored_{column}")) for column in VERSION_COLUMNS]
    )
    return (
        with_versions(changes, key, starts_at)
        .where(~F.col("__stored") | differs)
        .select(*columns, *VERSION_COLUMNS)
    )
