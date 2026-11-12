import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    PRODUCTS,
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
from pipelines.lake.gold.model.dimensions import dimension_versions
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VERSION_COLUMNS, version_changes

VERSION = ("_commit_sequence", *VERSION_COLUMNS)


def test_a_repeat_is_folded_into_the_version_it_repeats_and_ends_with_it(
    spark: SparkSession,
) -> None:
    batch = staged(
        spark,
        CUSTOMERS,
        [customer(1, 1, "Ada"), customer(1, 2, "Ada"), customer(1, 3, "Grace")],
    )

    versions = dimension_versions(CUSTOMERS, batch, stored(spark, CUSTOMERS.table))

    assert rows_of(versions, *VERSION) == [
        (1, False, 1, at(3), False),
        (2, True, 1, at(3), False),
        (3, False, 2, None, True),
    ]


def test_the_surrogate_key_is_a_hash_of_the_business_key_and_commit_sequence(
    spark: SparkSession,
) -> None:
    batch = staged(spark, CUSTOMERS, [customer(7, 42, "Ada")])

    versions = dimension_versions(CUSTOMERS, batch, stored(spark, CUSTOMERS.table))
    expected = spark.range(1).select(F.xxhash64(F.lit(7).cast("long"), F.lit(42).cast("long")))

    assert only(versions)["customer_key"] == only(expected)[0]


def test_a_late_version_splits_the_validity_of_the_versions_around_it(
    spark: SparkSession,
) -> None:
    first = dimension_versions(
        CUSTOMERS,
        staged(spark, CUSTOMERS, [customer(1, 1, "Ada"), customer(1, 3, "Grace")]),
        stored(spark, CUSTOMERS.table),
    )

    versions = dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, [customer(1, 2, "Hedy")]), first
    )

    assert rows_of(versions, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, at(3), False),
        (3, False, 3, None, True),
    ]


def test_a_late_version_between_two_equal_ones_unfolds_the_later_one(
    spark: SparkSession,
) -> None:
    first = dimension_versions(
        CUSTOMERS,
        staged(spark, CUSTOMERS, [customer(1, 1, "Ada"), customer(1, 3, "Ada")]),
        stored(spark, CUSTOMERS.table),
    )

    versions = dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, [customer(1, 2, "Grace")]), first
    )

    assert by_sequence(first)[3]["_is_folded"] is True
    assert rows_of(versions, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, at(3), False),
        (3, False, 3, None, True),
    ]
    assert rows_of(version_changes(versions, first, CUSTOMERS.key), COMMIT_SEQUENCE) == [
        (1,),
        (2,),
        (3,),
    ]


def test_a_delete_is_the_last_version_and_leaves_no_current_one(spark: SparkSession) -> None:
    batch = staged(spark, PRODUCTS, [product(1, 1, "9.99"), product(1, 2, "9.99", deleted=True)])

    versions = dimension_versions(PRODUCTS, batch, stored(spark, PRODUCTS.table))

    assert rows_of(versions, *VERSION) == [
        (1, False, 1, at(2), False),
        (2, False, 2, None, False),
    ]


def test_only_the_keys_the_batch_touches_are_recomputed(spark: SparkSession) -> None:
    first = dimension_versions(
        CUSTOMERS,
        staged(spark, CUSTOMERS, [customer(1, 1, "Ada"), customer(2, 2, "Grace")]),
        stored(spark, CUSTOMERS.table),
    )

    versions = dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, [customer(1, 3, "Hedy")]), first
    )

    assert rows_of(versions, "customer_id", COMMIT_SEQUENCE) == [(1, 1), (1, 3)]


def test_a_replayed_batch_keeps_the_stored_row_and_writes_nothing(spark: SparkSession) -> None:
    rows = [customer(1, 1, "Ada"), customer(1, 2, "Grace")]
    first = dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, rows), stored(spark, CUSTOMERS.table)
    )
    renamed = [customer(1, 2, "Changed on replay")]

    replayed = dimension_versions(CUSTOMERS, staged(spark, CUSTOMERS, [*rows, *renamed]), first)

    assert version_changes(replayed, first, CUSTOMERS.key).count() == 0
    assert sorted(replayed.collect()) == sorted(first.collect())


def test_a_change_delivered_twice_in_one_batch_is_one_version(spark: SparkSession) -> None:
    batch = staged(spark, CUSTOMERS, [customer(1, 1, "Ada"), customer(1, 1, "Ada")])

    versions = dimension_versions(CUSTOMERS, batch, stored(spark, CUSTOMERS.table))

    assert rows_of(versions, *VERSION) == [(1, False, 1, None, True)]


def test_merging_batch_by_batch_equals_loading_every_change_at_once(spark: SparkSession) -> None:
    changes = [
        customer(1, 4, "Ada"),
        customer(1, 1, "Ada"),
        customer(1, 3, "Grace", deleted=True),
        customer(1, 2, "Ada"),
        customer(2, 5, "Hedy"),
    ]
    table = stored(spark, CUSTOMERS.table)
    for change in changes:
        versions = dimension_versions(CUSTOMERS, staged(spark, CUSTOMERS, [change]), table)
        written = version_changes(versions, table, CUSTOMERS.key)
        table = merged(table, written, (CUSTOMERS.key, COMMIT_SEQUENCE)).localCheckpoint()

    at_once = dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, changes), stored(spark, CUSTOMERS.table)
    )

    assert sorted(table.collect()) == sorted(at_once.collect())
