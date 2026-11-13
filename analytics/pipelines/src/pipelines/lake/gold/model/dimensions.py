from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.model.columns import unversioned
from pipelines.lake.gold.model.versions import (
    COMMIT_SEQUENCE,
    VALID_FROM,
    touched,
    version_changes,
)
from pipelines.lake.gold.scd2 import Scd2Dimension


def surrogate_key(key: str) -> Column:
    """A version's surrogate key, hashed from its business key and `_commit_sequence`, so a
    rebuild gives every version the same key."""
    return F.xxhash64(F.col(key), F.col(COMMIT_SEQUENCE))


def dimension_changes(dimension: Scd2Dimension, batch: DataFrame, stored: DataFrame) -> DataFrame:
    """The versions a batch writes to the dimension: the new ones, and the stored ones of the
    keys it touches whose version columns change (`version_changes`).

    `batch` holds the dimension's staged columns, as `staged_rows` selects them from silver;
    `stored` holds the dimension's stored rows, at least those of the touched keys. The result
    depends only on the rows stored and the batch, so a replayed batch writes nothing new.
    """
    key = dimension.key
    columns = unversioned(column.name for column in dimension.table.columns)
    staged = batch.select(*(column.name for column in dimension.staged_columns))
    changes = staged.withColumn(dimension.surrogate_key, surrogate_key(key)).select(*columns)
    return version_changes(touched(stored, batch, key), changes, key, VALID_FROM)
