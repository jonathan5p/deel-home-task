import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import rows_of
from pipelines.lake.gold.model.versions import changed_rows, new_changes

ROWS = "id long, _commit_sequence long, _content_hash string, _is_deleted boolean, value long"


def test_changed_rows_are_the_new_rows_and_those_with_other_values(spark: SparkSession) -> None:
    before = spark.createDataFrame(
        [(1, 1, "a", False, None), (2, 1, "a", False, 5), (3, 1, "a", False, 5)], ROWS
    )
    after = spark.createDataFrame(
        [
            (1, 1, "a", False, None),
            (2, 1, "a", False, 6),
            (3, 1, "a", False, None),
            (4, 1, "a", False, 1),
        ],
        ROWS,
    )

    changed = changed_rows(after, before, ("id", "_commit_sequence"), ("value",))

    assert rows_of(changed, "id", "value") == [(2, 6), (3, None), (4, 1)]


def test_new_changes_skip_stored_changes_and_keep_one_of_each_repeated_change(
    spark: SparkSession,
) -> None:
    stored = spark.createDataFrame([(1, 1, "a", False, 0)], ROWS)
    batch = spark.createDataFrame(
        [(1, 1, "b", False, 1), (1, 2, "a", False, 2), (1, 2, "a", False, 2), (2, 1, "a", True, 3)],
        ROWS,
    )

    added = new_changes(batch, stored, "id")

    assert rows_of(added, "id", "_commit_sequence", "value") == [(1, 2, 2), (2, 1, 3)]
