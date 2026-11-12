from collections.abc import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.lake.gold.facts import AsOfLookup, ChangeFact
from pipelines.lake.gold.model.lookups import with_as_of_lookup, with_key_lookups
from pipelines.lake.gold.model.versions import (
    CHANGED_AT,
    COMMIT_SEQUENCE,
    CONTENT_HASH,
    VERSION_COLUMNS,
    changed_rows,
    new_changes,
    with_versions,
)
from pipelines.schemas.gold_definitions import GoldTable


def _names(table: GoldTable) -> list[str]:
    return [column.name for column in table.columns]


def _staged(fact: ChangeFact, batch: DataFrame) -> DataFrame:
    return batch.select(*(column.name for column in fact.staged_columns))


def history_rows(
    fact: ChangeFact,
    batch: DataFrame,
    stored: DataFrame,
    seeded: Mapping[str, DataFrame],
    dimensions: Mapping[str, DataFrame],
) -> DataFrame:
    """Every change of the keys `batch` touches, the stored ones and the new ones, as history
    rows with the version columns recomputed from all of them (`with_versions`).

    A new change gets its status and date keys from the seeded dimensions in `seeded`, and its
    as-of columns from the stored versions in `dimensions`, both by table name. A stored change
    keeps its looked-up values: only a dimension batch changes them (`resolved_history`).

    `batch` holds the fact's staged columns, as `staged_rows` selects them from silver; `stored`
    holds the stored history rows, at least those of the touched keys. A replayed batch
    recomputes the same rows; `version_changes` picks the rows to write.
    """
    key = fact.key
    columns = _names(fact.history)
    stored_rows = stored.join(batch.select(key).distinct(), key, "left_semi").select(*columns)
    added = new_changes(_staged(fact, batch), stored_rows, key)
    lookups = [lookup for lookup in fact.lookups if lookup.column in columns]
    added = with_key_lookups(added, lookups, seeded)
    for as_of in fact.as_of:
        added = with_as_of_lookup(added, key, as_of, dimensions[as_of.dimension.name])
    rows = stored_rows.unionByName(added.select(*columns))
    return with_versions(rows, key, CHANGED_AT).select(*columns, *VERSION_COLUMNS)


def resolved_history(
    fact: ChangeFact, lookup: AsOfLookup, history: DataFrame, versions: DataFrame
) -> DataFrame:
    """The history rows whose `lookup` columns resolve to other values after a dimension batch,
    as the fact's key, `_commit_sequence` and those columns.

    A late dimension version can split a validity range or unfold a version, and a dimension
    row can arrive after the facts that need it, so the values resolved when a change was
    written can go stale. `versions` holds every version of the keys the dimension batch
    touched (`dimension_versions`); `history` the stored history rows, at least those whose
    business key is one of them.

    The writer merges the result on the key and `_commit_sequence`: when matched, update the
    looked-up columns.
    """
    identity = (fact.key, COMMIT_SEQUENCE)
    touched = versions.select(F.col(lookup.dimension.key).alias(lookup.business_key)).distinct()
    affected = history.join(touched, lookup.business_key, "left_semi")
    resolved = with_as_of_lookup(affected, fact.key, lookup, versions)
    columns = lookup.fact_columns
    return changed_rows(resolved, affected, identity, columns).select(*identity, *columns)


def current_rows(
    fact: ChangeFact, batch: DataFrame, stored: DataFrame, seeded: Mapping[str, DataFrame]
) -> DataFrame:
    """The latest change of each key in `batch` as a current-state row, where it is newer than
    the stored row: the key is not stored yet, or the change has a higher `_commit_sequence`.
    A late or replayed change never overwrites newer state. A delete stays as a deleted row
    with its `_commit_sequence`, so an older change arriving later cannot bring the row back.

    The status, its `is_open` and the date keys come from the seeded dimensions in `seeded`.
    `stored` holds the stored current-state rows, at least those of the batch's keys.

    The writer merges the result on the key: when matched and the incoming `_commit_sequence`
    is higher, update every column; when not matched, insert the row. The condition repeats
    the filter here, so a row that a concurrent write made stale is still not applied.
    """
    key = fact.key
    columns = _names(fact.current)
    latest = Window.partitionBy(key).orderBy(F.col(COMMIT_SEQUENCE).desc(), CONTENT_HASH)
    rows = (
        _staged(fact, batch)
        .withColumn("__recency", F.row_number().over(latest))
        .where(F.col("__recency") == 1)
    )
    lookups = [lookup for lookup in fact.lookups if lookup.column in columns]
    rows = with_key_lookups(rows, lookups, seeded).select(*columns)
    stored_sequence = stored.select(key, F.col(COMMIT_SEQUENCE).alias("__stored_sequence"))
    return (
        rows.join(stored_sequence, key, "left")
        .where(
            F.col("__stored_sequence").isNull()
            | (F.col(COMMIT_SEQUENCE) > F.col("__stored_sequence"))
        )
        .select(*columns)
    )


def merged_current(stored: DataFrame, upserts: DataFrame, key: str) -> DataFrame:
    """`stored` current-state rows with the `current_rows` upserts applied, as the merge leaves
    them."""
    kept = stored.join(upserts.select(key), key, "left_anti").select(*stored.columns)
    return kept.unionByName(upserts.select(*stored.columns))
