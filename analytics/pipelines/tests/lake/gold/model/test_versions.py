import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import at, rows_of
from pipelines.lake.gold.model.versions import touched, version_changes

ADDED = (
    "id long, _commit_sequence long, _content_hash string, _is_deleted boolean,"
    " _changed_at timestamp"
)
STORED = (
    f"{ADDED}, _is_folded boolean, _version_number long, _valid_to timestamp, _is_current boolean"
)


def test_version_changes_are_the_new_rows_and_the_stored_ones_they_re_place(
    spark: SparkSession,
) -> None:
    stored = spark.createDataFrame(
        [
            (1, 1, "a", False, at(1), False, 1, None, True),
            (2, 1, "a", False, at(1), False, 1, None, True),
        ],
        STORED,
    )
    batch = spark.createDataFrame([(1, 2, "b", False, at(2))], ADDED)

    changes = version_changes(stored, batch, "id", "_changed_at")

    assert rows_of(changes, "id", "_commit_sequence", "_valid_to", "_is_current") == [
        (1, 1, at(2), False),
        (1, 2, None, True),
    ]


def test_a_stored_change_delivered_again_keeps_the_stored_row(spark: SparkSession) -> None:
    stored = spark.createDataFrame([(1, 1, "a", False, at(1), False, 1, None, True)], STORED)
    batch = spark.createDataFrame([(1, 1, "changed", False, at(1))], ADDED)

    assert version_changes(stored, batch, "id", "_changed_at").count() == 0


def test_a_change_delivered_twice_counts_once_with_the_lowest_hash(spark: SparkSession) -> None:
    stored = spark.createDataFrame([], STORED)
    batch = spark.createDataFrame([(1, 1, "b", False, at(1)), (1, 1, "a", False, at(1))], ADDED)

    changes = version_changes(stored, batch, "id", "_changed_at")

    assert rows_of(changes, "id", "_content_hash", "_version_number") == [(1, "a", 1)]


def test_touched_rows_are_the_stored_rows_of_the_batchs_keys(spark: SparkSession) -> None:
    stored = spark.createDataFrame([(1, "a"), (2, "b")], "id long, value string")
    batch = spark.createDataFrame([(2,), (2,), (3,)], "id long")

    assert rows_of(touched(stored, batch, "id"), "id", "value") == [(2, "b")]
