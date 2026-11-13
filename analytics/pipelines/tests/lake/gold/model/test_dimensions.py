import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    PRODUCTS,
    Values,
    at,
    by_sequence,
    customer,
    merged,
    only,
    product,
    rows_of,
    staged,
    stored,
)
from pipelines.lake.gold.model.dimensions import dimension_changes
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VERSION_COLUMNS
from pipelines.lake.gold.scd2 import Scd2Dimension

VERSION = ("_commit_sequence", *VERSION_COLUMNS)


def changes(
    spark: SparkSession,
    rows: list[Values],
    table: DataFrame | None = None,
    dimension: Scd2Dimension = CUSTOMERS,
) -> DataFrame:
    stored_rows = stored(spark, dimension.table) if table is None else table
    return dimension_changes(dimension, staged(spark, dimension, rows), stored_rows)


def merged_into(table: DataFrame, written: DataFrame) -> DataFrame:
    return merged(table, written, (CUSTOMERS.key, COMMIT_SEQUENCE)).localCheckpoint()


def loaded(spark: SparkSession, rows: list[Values]) -> DataFrame:
    """The dimension table after one batch of `rows`."""
    return merged_into(stored(spark, CUSTOMERS.table), changes(spark, rows))


def test_a_repeat_is_folded_into_the_version_it_repeats_and_ends_with_it(
    spark: SparkSession,
) -> None:
    written = changes(
        spark, [customer(1, 1, "Ada"), customer(1, 2, "Ada"), customer(1, 3, "Grace")]
    )

    assert rows_of(written, *VERSION) == [
        (1, False, 1, at(3), False),
        (2, True, 1, at(3), False),
        (3, False, 2, None, True),
    ]


def test_the_surrogate_key_is_a_hash_of_the_business_key_and_commit_sequence(
    spark: SparkSession,
) -> None:
    written = changes(spark, [customer(7, 42, "Ada")])
    expected = spark.range(1).select(F.xxhash64(F.lit(7).cast("long"), F.lit(42).cast("long")))

    assert only(written)["customer_key"] == only(expected)[0]


def test_a_late_version_splits_the_validity_of_the_versions_around_it(
    spark: SparkSession,
) -> None:
    first = loaded(spark, [customer(1, 1, "Ada"), customer(1, 3, "Grace")])

    after = merged_into(first, changes(spark, [customer(1, 2, "Hedy")], first))

    assert rows_of(after, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, at(3), False),
        (3, False, 3, None, True),
    ]


def test_a_late_version_between_two_equal_ones_unfolds_the_later_one(
    spark: SparkSession,
) -> None:
    first = loaded(spark, [customer(1, 1, "Ada"), customer(1, 3, "Ada")])

    written = changes(spark, [customer(1, 2, "Grace")], first)

    assert by_sequence(first)[3]["_is_folded"] is True
    assert rows_of(written, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, at(3), False),
        (3, False, 3, None, True),
    ]


def test_a_delete_is_the_last_version_and_leaves_no_current_one(spark: SparkSession) -> None:
    rows = [product(1, 1, "9.99"), product(1, 2, "9.99", deleted=True)]

    written = changes(spark, rows, dimension=PRODUCTS)

    assert rows_of(written, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, None, False),
    ]


def test_only_the_versions_a_batch_re_places_are_written_again(spark: SparkSession) -> None:
    first = loaded(spark, [customer(1, 1, "Ada"), customer(2, 2, "Grace")])

    written = changes(spark, [customer(1, 3, "Hedy")], first)

    assert rows_of(written, "customer_id", COMMIT_SEQUENCE) == [(1, 1), (1, 3)]


def test_a_replayed_batch_keeps_the_stored_row_and_writes_nothing(spark: SparkSession) -> None:
    rows = [customer(1, 1, "Ada"), customer(1, 2, "Grace")]
    first = loaded(spark, rows)

    replayed = changes(spark, [*rows, customer(1, 2, "Changed on replay")], first)

    assert replayed.count() == 0


def test_a_change_delivered_twice_in_one_batch_is_one_version(spark: SparkSession) -> None:
    written = changes(spark, [customer(1, 1, "Ada"), customer(1, 1, "Ada")])

    assert rows_of(written, *VERSION) == [(1, False, 1, None, True)]


def test_merging_batch_by_batch_equals_loading_every_change_at_once(spark: SparkSession) -> None:
    rows = [
        customer(1, 4, "Ada"),
        customer(1, 1, "Ada"),
        customer(1, 3, "Grace", deleted=True),
        customer(1, 2, "Ada"),
        customer(2, 5, "Hedy"),
    ]
    table = stored(spark, CUSTOMERS.table)
    for row in rows:
        table = merged_into(table, changes(spark, [row], table))

    assert sorted(table.collect()) == sorted(loaded(spark, rows).collect())
