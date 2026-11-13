import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    ORDERS,
    Values,
    at,
    merged,
    order,
    rows_of,
    seeded,
    staged,
    stored,
)
from pipelines.lake.gold.model.facts import current_rows, history_changes, looked_up
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VERSION_COLUMNS

VERSION = (COMMIT_SEQUENCE, *VERSION_COLUMNS)


def batch(spark: SparkSession, rows: list[Values]) -> DataFrame:
    return looked_up(ORDERS, staged(spark, ORDERS, rows), seeded(spark))


def history(spark: SparkSession, rows: list[Values], table: DataFrame | None = None) -> DataFrame:
    stored_rows = stored(spark, ORDERS.history) if table is None else table
    return history_changes(ORDERS, batch(spark, rows), stored_rows)


def history_after(spark: SparkSession, rows: list[Values], table: DataFrame) -> DataFrame:
    written = history(spark, rows, table)
    return merged(table, written, (ORDERS.key, COMMIT_SEQUENCE)).localCheckpoint()


def test_a_new_change_keeps_its_business_keys_and_gets_its_status_and_date_keys(
    spark: SparkSession,
) -> None:
    rows = history(spark, [order(1, 10, "COMPLETED", customer_id=7)])

    assert rows_of(
        rows, "order_id", "customer_id", "status_key", "order_date_key", "delivery_date_key"
    ) == [(1, 7, 4, 20261101, 20261120)]


def test_the_history_holds_no_dimension_keys_or_attributes(spark: SparkSession) -> None:
    columns = set(history(spark, [order(1, 10)]).columns)

    assert columns.isdisjoint({"customer_key", "customer_name", "customer_address"})


def test_history_rows_fold_repeats_and_end_each_version_at_the_next_one(
    spark: SparkSession,
) -> None:
    rows = history(
        spark, [order(1, 10, "PENDING"), order(1, 11, "PENDING"), order(1, 12, "COMPLETED")]
    )

    assert rows_of(rows, *VERSION) == [
        (10, False, 1, at(12), False),
        (11, True, 1, at(12), False),
        (12, False, 2, None, True),
    ]


def test_a_late_change_unfolds_the_next_one_and_writes_the_rows_it_re_places(
    spark: SparkSession,
) -> None:
    first = history_after(
        spark, [order(1, 10, "PENDING"), order(1, 12, "PENDING")], stored(spark, ORDERS.history)
    )

    written = history(spark, [order(1, 11, "COMPLETED")], first)

    assert rows_of(written, *VERSION) == [
        (10, False, 1, at(11), False),
        (11, False, 2, at(12), False),
        (12, False, 3, None, True),
    ]


def test_a_delete_is_the_last_history_row_and_is_never_current(spark: SparkSession) -> None:
    rows = history(spark, [order(1, 10), order(1, 11, deleted=True)])

    assert rows_of(rows, COMMIT_SEQUENCE, "_is_current", "_valid_to") == [
        (10, False, at(11)),
        (11, False, None),
    ]


def test_a_replayed_history_batch_writes_nothing(spark: SparkSession) -> None:
    rows = [order(1, 10, "PENDING"), order(1, 11, "COMPLETED")]
    first = history_after(spark, rows, stored(spark, ORDERS.history))

    assert history(spark, rows, first).count() == 0


def test_a_batch_writes_only_the_history_of_the_orders_it_touches(spark: SparkSession) -> None:
    first = history_after(spark, [order(1, 10), order(2, 11)], stored(spark, ORDERS.history))

    written = history(spark, [order(1, 12, "COMPLETED")], first)

    assert rows_of(written, "order_id", COMMIT_SEQUENCE) == [(1, 10), (1, 12)]


def current(spark: SparkSession, rows: list[Values], table: DataFrame | None = None) -> DataFrame:
    stored_rows = stored(spark, ORDERS.current, versioned=False) if table is None else table
    return current_rows(ORDERS, batch(spark, rows), stored_rows)


def test_the_current_state_takes_the_latest_change_and_only_when_it_is_newer(
    spark: SparkSession,
) -> None:
    table = current(spark, [order(1, 5), order(3, 4)])

    upserts = current(
        spark, [order(1, 3, "COMPLETED"), order(2, 1), order(3, 6), order(3, 7, "COMPLETED")], table
    )

    assert rows_of(upserts, "order_id", COMMIT_SEQUENCE, "status_key") == [(2, 1, 1), (3, 7, 4)]


def test_is_open_is_false_only_for_completed_orders(spark: SparkSession) -> None:
    upserts = current(
        spark, [order(1, 1, "COMPLETED"), order(2, 2, "PROCESSING"), order(3, 3, None)]
    )

    assert rows_of(upserts, "order_id", "status_key", "is_open") == [
        (1, 4, False),
        (2, 2, True),
        (3, 5, True),
    ]


def test_a_deleted_order_stays_deleted_when_an_older_change_arrives(spark: SparkSession) -> None:
    table = current(spark, [order(1, 5), order(1, 6, deleted=True)])

    late = current(spark, [order(1, 4, "COMPLETED")], table)

    assert rows_of(table, "order_id", COMMIT_SEQUENCE, "_is_deleted") == [(1, 6, True)]
    assert late.count() == 0


def test_a_replayed_current_state_batch_writes_nothing(spark: SparkSession) -> None:
    rows = [order(1, 5), order(2, 6)]
    table = current(spark, rows)

    assert current(spark, rows, table).count() == 0
