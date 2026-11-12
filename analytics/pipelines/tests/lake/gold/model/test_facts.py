import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMER,
    CUSTOMERS,
    ORDERS,
    Values,
    at,
    by_sequence,
    customer,
    only,
    order,
    rows_of,
    seeded,
    staged,
    stored,
)
from pipelines.lake.gold.model.dimensions import dimension_versions
from pipelines.lake.gold.model.facts import (
    current_rows,
    history_rows,
    merged_current,
    resolved_history,
)
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VERSION_COLUMNS, version_changes

VERSION = (COMMIT_SEQUENCE, *VERSION_COLUMNS)


def customer_versions(
    spark: SparkSession, rows: list[Values], table: DataFrame | None = None
) -> DataFrame:
    stored_rows = stored(spark, CUSTOMERS.table) if table is None else table
    return dimension_versions(CUSTOMERS, staged(spark, CUSTOMERS, rows), stored_rows)


def history(
    spark: SparkSession, rows: list[Values], customers: DataFrame, table: DataFrame | None = None
) -> DataFrame:
    stored_rows = stored(spark, ORDERS.history) if table is None else table
    return history_rows(
        ORDERS,
        staged(spark, ORDERS, rows),
        stored_rows,
        seeded(spark),
        {"dim_customer": customers},
    )


def test_a_new_change_gets_its_status_date_and_customer_keys(spark: SparkSession) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada")])
    customer_key = only(customers)["customer_key"]

    rows = history(spark, [order(1, 10, "COMPLETED")], customers)

    assert rows_of(
        rows, "order_id", "status_key", "customer_key", "order_date_key", "delivery_date_key"
    ) == [(1, 4, customer_key, 20261101, 20261120)]


def test_history_rows_fold_repeats_and_end_each_version_at_the_next_one(
    spark: SparkSession,
) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada")])

    rows = history(
        spark,
        [order(1, 10, "PENDING"), order(1, 11, "PENDING"), order(1, 12, "COMPLETED")],
        customers,
    )

    assert rows_of(rows, *VERSION) == [
        (10, False, 1, at(12), False),
        (11, True, 1, at(12), False),
        (12, False, 2, None, True),
    ]


def test_a_late_change_unfolds_the_next_one_and_keeps_the_stored_lookups(
    spark: SparkSession,
) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada")])
    first = history(spark, [order(1, 10, "PENDING"), order(1, 12, "PENDING")], customers)

    rows = history(spark, [order(1, 11, "COMPLETED")], customers, first)

    assert rows_of(rows, *VERSION) == [
        (10, False, 1, at(11), False),
        (11, False, 2, at(12), False),
        (12, False, 3, None, True),
    ]
    assert rows_of(version_changes(rows, first, ORDERS.key), COMMIT_SEQUENCE) == [
        (10,),
        (11,),
        (12,),
    ]
    assert by_sequence(rows)[12]["customer_key"] == by_sequence(first)[12]["customer_key"]


def test_a_delete_is_the_last_history_row_and_is_never_current(spark: SparkSession) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada")])

    rows = history(spark, [order(1, 10), order(1, 11, deleted=True)], customers)

    assert rows_of(rows, COMMIT_SEQUENCE, "_is_current", "_valid_to") == [
        (10, False, at(11)),
        (11, False, None),
    ]


def test_a_replayed_history_batch_writes_nothing(spark: SparkSession) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada")])
    batch = [order(1, 10, "PENDING"), order(1, 11, "COMPLETED")]
    first = history(spark, batch, customers)

    replayed = history(spark, batch, customers, first)

    assert version_changes(replayed, first, ORDERS.key).count() == 0


def test_a_late_dimension_version_re_resolves_only_the_history_rows_it_changes(
    spark: SparkSession,
) -> None:
    customers = customer_versions(spark, [customer(1, 1, "Ada"), customer(2, 2, "Hedy")])
    stored_history = history(
        spark,
        [order(1, 10, minute=10), order(1, 30, "COMPLETED", minute=30), order(2, 5, customer_id=2)],
        customers,
    )

    late = customer_versions(spark, [customer(1, 20, "Grace", minute=20)], customers)
    resolved = resolved_history(ORDERS, CUSTOMER, stored_history, late)

    assert rows_of(resolved, "order_id", COMMIT_SEQUENCE, "customer_key") == [
        (1, 30, by_sequence(late)[20]["customer_key"])
    ]


def test_a_dimension_row_arriving_after_its_facts_fills_their_keys(spark: SparkSession) -> None:
    stored_history = history(spark, [order(1, 10)], stored(spark, CUSTOMERS.table))

    arrived = customer_versions(spark, [customer(1, 20, "Ada", minute=20)])
    resolved = resolved_history(ORDERS, CUSTOMER, stored_history, arrived)

    assert rows_of(stored_history, "customer_key") == [(None,)]
    assert rows_of(resolved, "order_id", "customer_key") == [(1, only(arrived)["customer_key"])]


def current(spark: SparkSession, rows: list[Values], table: DataFrame | None = None) -> DataFrame:
    stored_rows = stored(spark, ORDERS.current, versioned=False) if table is None else table
    return current_rows(ORDERS, staged(spark, ORDERS, rows), stored_rows, seeded(spark))


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
    batch = [order(1, 5), order(2, 6)]
    table = current(spark, batch)

    assert current(spark, batch, table).count() == 0


def test_merged_current_replaces_the_upserted_rows_and_keeps_the_others(
    spark: SparkSession,
) -> None:
    table = current(spark, [order(1, 5), order(2, 6)])
    upserts = current(spark, [order(2, 7, "COMPLETED"), order(3, 8)], table)

    after = merged_current(table, upserts, ORDERS.key)

    assert rows_of(after, "order_id", COMMIT_SEQUENCE) == [(1, 5), (2, 7), (3, 8)]
