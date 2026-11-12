from datetime import date

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    ITEMS,
    ORDERS,
    customer,
    item,
    order,
    rows_of,
    staged,
    stored,
)
from analytics.pipelines.tests.lake.gold.model.report_samples import CurrentState, merged_report
from pipelines.lake.gold.model.dimensions import dimension_versions
from pipelines.lake.gold.model.reports import (
    IS_REMOVED,
    open_orders_by_delivery_date_and_status,
    pending_items_by_product,
    pending_orders_by_customer,
    products_of_orders,
    top_customers_by_pending_orders,
    top_delivery_dates_by_open_orders,
)
from pipelines.lake.gold.model.seeds import order_statuses

FIRST_DAY = date(2026, 11, 20)
SECOND_DAY = date(2026, 11, 21)
BY_DATE = ("delivery_date", "status_code", "open_orders", IS_REMOVED)
KEYS_BY_DATE = ("delivery_date", "status_code")
BY_CUSTOMER = ("customer_id", "pending_orders", IS_REMOVED)
BY_PRODUCT = ("product_id", "pending_items", "pending_quantity", IS_REMOVED)


def test_an_order_moving_to_another_delivery_date_updates_both_dates(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    orders.apply(
        [
            order(1, 1, delivery_date=FIRST_DAY),
            order(2, 2, delivery_date=FIRST_DAY),
            order(3, 3, delivery_date=SECOND_DAY),
        ]
    )

    changed = orders.apply([order(1, 4, delivery_date=SECOND_DAY)])
    report = open_orders_by_delivery_date_and_status(changed, orders.rows, order_statuses(spark))

    assert rows_of(report, *BY_DATE) == [
        (FIRST_DAY, "PENDING", 1, False),
        (SECOND_DAY, "PENDING", 2, False),
    ]


def test_a_completed_order_lowers_every_count_it_was_in(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    orders.apply([order(1, 1, customer_id=7), order(2, 2, customer_id=7)])
    items = CurrentState(spark, ITEMS)
    items.apply(
        [item(1, 1, order_id=1, product_id=9, quantity=2), item(2, 2, order_id=2, product_id=9)]
    )

    changed = orders.apply([order(1, 3, "COMPLETED", customer_id=7)])
    products = products_of_orders(changed, items.rows)

    by_date = open_orders_by_delivery_date_and_status(changed, orders.rows, order_statuses(spark))
    assert rows_of(by_date, *BY_DATE) == [(FIRST_DAY, "PENDING", 1, False)]
    by_customer = pending_orders_by_customer(changed, orders.rows)
    assert rows_of(by_customer, *BY_CUSTOMER) == [(7, 1, False)]
    by_product = pending_items_by_product(products, items.rows, orders.rows)
    assert rows_of(by_product, *BY_PRODUCT) == [(9, 1, 2, False)]


def test_a_total_that_drops_to_zero_removes_its_row(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    orders.apply([order(1, 1, customer_id=7)])
    items = CurrentState(spark, ITEMS)
    items.apply([item(1, 1, order_id=1, product_id=9)])

    changed = orders.apply([order(1, 2, "PROCESSING", customer_id=8, deleted=True)])
    products = products_of_orders(changed, items.rows)

    by_date = open_orders_by_delivery_date_and_status(changed, orders.rows, order_statuses(spark))
    assert rows_of(by_date, *BY_DATE) == [(FIRST_DAY, "PENDING", None, True)]
    by_customer = pending_orders_by_customer(changed, orders.rows)
    assert rows_of(by_customer, *BY_CUSTOMER) == [(7, None, True)]
    by_product = pending_items_by_product(products, items.rows, orders.rows)
    assert rows_of(by_product, *BY_PRODUCT) == [(9, None, None, True)]


def test_an_order_changing_status_moves_its_count_to_the_new_status(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    orders.apply([order(1, 1, "PENDING")])

    changed = orders.apply([order(1, 2, "PROCESSING")])
    report = open_orders_by_delivery_date_and_status(changed, orders.rows, order_statuses(spark))

    assert rows_of(report, *BY_DATE) == [
        (FIRST_DAY, "PENDING", None, True),
        (FIRST_DAY, "PROCESSING", 1, False),
    ]


def test_an_order_without_a_delivery_date_is_counted_under_a_null_date(
    spark: SparkSession,
) -> None:
    orders = CurrentState(spark, ORDERS)
    first = orders.apply([order(1, 1, delivery_date=None), order(2, 2, delivery_date=None)])
    opened = open_orders_by_delivery_date_and_status(first, orders.rows, order_statuses(spark))

    changed = orders.apply([order(1, 3, "COMPLETED", delivery_date=None)])
    report = open_orders_by_delivery_date_and_status(changed, orders.rows, order_statuses(spark))

    assert rows_of(opened, *BY_DATE) == [(None, "PENDING", 2, False)]
    assert rows_of(report, *BY_DATE) == [(None, "PENDING", 1, False)]


def test_an_item_moving_to_another_product_updates_both_products(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    orders.apply([order(1, 1)])
    items = CurrentState(spark, ITEMS)
    items.apply([item(1, 1, product_id=9, quantity=4), item(2, 2, product_id=9, quantity=None)])

    changed = items.apply([item(1, 3, product_id=8, quantity=4)])
    report = pending_items_by_product(changed.select("product_id"), items.rows, orders.rows)

    assert rows_of(report, *BY_PRODUCT) == [(8, 1, 4, False), (9, 1, None, False)]


def test_a_replayed_batch_recounts_the_same_numbers(spark: SparkSession) -> None:
    orders = CurrentState(spark, ORDERS)
    statuses = order_statuses(spark)
    initial = orders.apply(
        [order(1, 1, delivery_date=FIRST_DAY), order(2, 2, delivery_date=SECOND_DAY)]
    )
    table = merged_report(
        open_orders_by_delivery_date_and_status(initial, orders.rows, statuses), KEYS_BY_DATE
    )
    batch = [order(1, 3, delivery_date=SECOND_DAY), order(3, 4, "COMPLETED")]

    first = open_orders_by_delivery_date_and_status(orders.apply(batch), orders.rows, statuses)
    after_first = merged_report(first, KEYS_BY_DATE, table).localCheckpoint()
    replayed = open_orders_by_delivery_date_and_status(orders.apply(batch), orders.rows, statuses)
    after_replay = merged_report(replayed, KEYS_BY_DATE, after_first)

    assert rows_of(after_first, *BY_DATE[:-1]) == [(SECOND_DAY, "PENDING", 2)]
    assert rows_of(after_replay, *BY_DATE[:-1]) == rows_of(after_first, *BY_DATE[:-1])


def test_the_top_delivery_dates_skip_the_place_after_a_tie(spark: SparkSession) -> None:
    counts = [
        (date(2026, 11, 1), "PENDING", 3),
        (date(2026, 11, 1), "PROCESSING", 2),
        (date(2026, 11, 2), "PENDING", 4),
        (date(2026, 11, 3), "PENDING", 4),
        (date(2026, 11, 4), "PENDING", 3),
        (date(2026, 11, 5), "PENDING", 2),
    ]
    report = spark.createDataFrame(
        counts, "delivery_date date, status_code string, open_orders long"
    )

    top = top_delivery_dates_by_open_orders(report)

    assert rows_of(top, "delivery_date", "open_orders") == [
        (date(2026, 11, 1), 5),
        (date(2026, 11, 2), 4),
        (date(2026, 11, 3), 4),
    ]


def test_the_top_delivery_dates_keep_every_date_tied_for_third_place(spark: SparkSession) -> None:
    counts = [
        (date(2026, 11, 1), "PENDING", 5),
        (date(2026, 11, 2), "PENDING", 4),
        (date(2026, 11, 3), "PENDING", 3),
        (date(2026, 11, 4), "PENDING", 3),
        (date(2026, 11, 5), "PENDING", 2),
    ]
    report = spark.createDataFrame(
        counts, "delivery_date date, status_code string, open_orders long"
    )

    top = top_delivery_dates_by_open_orders(report)

    assert rows_of(top, "delivery_date", "open_orders") == [
        (date(2026, 11, 1), 5),
        (date(2026, 11, 2), 4),
        (date(2026, 11, 3), 3),
        (date(2026, 11, 4), 3),
    ]


def test_the_top_customers_keep_ties_and_take_the_current_name(spark: SparkSession) -> None:
    pending = spark.createDataFrame(
        [(1, 5), (2, 5), (3, 4), (4, 3), (5, 1)], "customer_id long, pending_orders long"
    )
    customers = dimension_versions(
        CUSTOMERS,
        staged(
            spark,
            CUSTOMERS,
            [customer(1, 1, "Ada"), customer(1, 2, "Ada Lovelace"), customer(2, 3, "Grace")],
        ),
        stored(spark, CUSTOMERS.table),
    )

    top = top_customers_by_pending_orders(pending, customers)

    assert rows_of(top, "customer_id", "customer_name", "pending_orders") == [
        (1, "Ada Lovelace", 5),
        (2, "Grace", 5),
        (3, None, 4),
    ]
