from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.model.columns import unversioned
from pipelines.lake.gold.model.versions import (
    COMMIT_SEQUENCE,
    VALID_FROM,
    VERSION_COLUMNS,
    new_changes,
    with_versions,
)
from pipelines.lake.gold.scd2 import Scd2Dimension


def surrogate_key(key: str) -> Column:
    """A version's surrogate key, hashed from its business key and `_commit_sequence`, so a
    rebuild gives every version the same key."""
    return F.xxhash64(F.col(key), F.col(COMMIT_SEQUENCE))


def dimension_versions(dimension: Scd2Dimension, batch: DataFrame, stored: DataFrame) -> DataFrame:
    """Every version of the keys `batch` touches, the stored ones and the new ones, with the
    version columns recomputed from all of them (`with_versions`).

    `batch` holds the dimension's staged columns, as `staged_rows` selects them from silver;
    `stored` holds the dimension's stored rows, at least those of the touched keys. The result
    depends only on the rows stored and the batch, so a replayed batch recomputes the same
    versions. `version_changes` picks the rows to write from it.
    """
    key = dimension.key
    columns = unversioned(column.name for column in dimension.table.columns)
    stored_rows = stored.join(batch.select(key).distinct(), key, "left_semi").select(*columns)
    staged = batch.select(*(column.name for column in dimension.staged_columns))
    added = new_changes(staged, stored_rows, key).withColumn(
        dimension.surrogate_key, surrogate_key(key)
    )
    rows = stored_rows.unionByName(added.select(*columns))
    return with_versions(rows, key, VALID_FROM).select(*columns, *VERSION_COLUMNS)
