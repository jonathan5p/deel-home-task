from collections.abc import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.model.columns import unversioned
from pipelines.lake.gold.model.lookups import with_key_lookups
from pipelines.lake.gold.model.versions import (
    CHANGED_AT,
    COMMIT_SEQUENCE,
    CONTENT_HASH,
    touched,
    version_changes,
)
from pipelines.schemas.gold_definitions import GoldTable


def _names(table: GoldTable) -> list[str]:
    return [column.name for column in table.columns]


def looked_up(fact: ChangeFact, batch: DataFrame, seeded: Mapping[str, DataFrame]) -> DataFrame:
    """The fact's staged columns of `batch`, as `staged_rows` selects them from silver, with the
    status and date keys of every change looked up in the seeded dimensions in `seeded`, by
    table name. The history and current-state rows are both selected from it."""
    staged = batch.select(*(column.name for column in fact.staged_columns))
    return with_key_lookups(staged, fact.lookups, seeded)


def history_changes(fact: ChangeFact, batch: DataFrame, stored: DataFrame) -> DataFrame:
    """The history rows a batch writes: its new changes, and the stored changes of the keys it
    touches whose version columns change (`version_changes`). A stored change keeps the values
    it was written with. Nothing is read from another entity's tables, so each fact's stream is
    independent of the others.

    `batch` holds the rows `looked_up` returns; `stored` holds the stored history rows, at
    least those of the touched keys. A replayed batch writes nothing new.
    """
    key = fact.key
    columns = unversioned(_names(fact.history))
    return version_changes(touched(stored, batch, key), batch.select(*columns), key, CHANGED_AT)


def current_rows(fact: ChangeFact, batch: DataFrame, stored: DataFrame) -> DataFrame:
    """The latest change of each key in `batch` as a current-state row, where it is newer than
    the stored row: the key is not stored yet, or the change has a higher `_commit_sequence`.
    A late or replayed change never overwrites newer state. A delete stays as a deleted row
    with its `_commit_sequence`, so an older change arriving later cannot bring the row back.

    `batch` holds the rows `looked_up` returns; `stored` holds the stored current-state rows,
    at least those of the batch's keys.

    The stored row and the batch's changes of each key are ranked in one pass, the stored row
    first on a tie. The writer merges the result on the key: when matched and the incoming
    `_commit_sequence` is higher, update every column; when not matched, insert the row.
    """
    key = fact.key
    columns = _names(fact.current)
    previous = touched(stored, batch, key).select(
        *columns, F.lit(None).cast("string").alias(CONTENT_HASH), F.lit(True).alias("__stored")
    )
    incoming = batch.select(*columns, CONTENT_HASH, F.lit(False).alias("__stored"))
    latest = Window.partitionBy(key).orderBy(
        F.col(COMMIT_SEQUENCE).desc(), F.col("__stored").desc(), CONTENT_HASH
    )
    return (
        previous.unionByName(incoming)
        .withColumn("__recency", F.row_number().over(latest))
        .where((F.col("__recency") == 1) & ~F.col("__stored"))
        .select(*columns)
    )
