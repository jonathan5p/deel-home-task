from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMER,
    CUSTOMERS,
    ITEMS,
    ORDERS,
    PRODUCT,
    PRODUCTS,
    Values,
    by_sequence,
    customer,
    item,
    order,
    product,
    rows_of,
    seeded,
    staged,
    stored,
)
from pipelines.lake.gold.model.dimensions import dimension_versions
from pipelines.lake.gold.model.lookups import with_as_of_lookup, with_key_lookups


def customer_versions(spark: SparkSession, rows: list[Values]) -> DataFrame:
    return dimension_versions(
        CUSTOMERS, staged(spark, CUSTOMERS, rows), stored(spark, CUSTOMERS.table)
    )


def customer_keys(spark: SparkSession, versions: DataFrame, orders: list[Values]) -> list[object]:
    looked_up = with_as_of_lookup(staged(spark, ORDERS, orders), ORDERS.key, CUSTOMER, versions)
    return [key for _, key in rows_of(looked_up, "order_id", "customer_key")]


def test_a_change_takes_the_version_valid_when_it_was_committed(spark: SparkSession) -> None:
    versions = customer_versions(spark, [customer(1, 10, "Ada"), customer(1, 20, "Grace")])
    keys = {sequence: row["customer_key"] for sequence, row in by_sequence(versions).items()}

    found = customer_keys(
        spark,
        versions,
        [
            order(1, 1, minute=15),
            order(2, 2, minute=20),
            order(3, 3, minute=25),
            order(4, 4, customer_id=2, minute=25),
            order(5, 5, customer_id=None, minute=25),
        ],
    )

    assert found == [keys[10], keys[20], keys[20], None, None]


def test_a_change_older_than_the_first_version_takes_the_first_version(
    spark: SparkSession,
) -> None:
    versions = customer_versions(spark, [customer(1, 10, "Ada"), customer(1, 20, "Grace")])

    found = customer_keys(spark, versions, [order(1, 1, minute=5)])

    assert found == [by_sequence(versions)[10]["customer_key"]]


def test_a_first_version_that_is_a_delete_covers_no_earlier_change(spark: SparkSession) -> None:
    versions = customer_versions(
        spark, [customer(1, 10, "Ada", deleted=True), customer(1, 20, "Grace")]
    )

    found = customer_keys(spark, versions, [order(1, 1, minute=5), order(2, 2, minute=25)])

    assert found == [None, by_sequence(versions)[20]["customer_key"]]


def test_a_change_after_a_delete_finds_no_version(spark: SparkSession) -> None:
    versions = customer_versions(
        spark, [customer(1, 10, "Ada"), customer(1, 20, "Ada", deleted=True)]
    )

    found = customer_keys(spark, versions, [order(1, 1, minute=15), order(2, 2, minute=25)])

    assert found == [by_sequence(versions)[10]["customer_key"], None]


def test_a_folded_version_is_never_taken(spark: SparkSession) -> None:
    versions = customer_versions(spark, [customer(1, 10, "Ada"), customer(1, 20, "Ada")])

    found = customer_keys(spark, versions, [order(1, 1, minute=25)])

    assert found == [by_sequence(versions)[10]["customer_key"]]


def test_the_line_amount_is_the_quantity_times_the_looked_up_price(spark: SparkSession) -> None:
    versions = dimension_versions(
        PRODUCTS, staged(spark, PRODUCTS, [product(1, 1, "2.5")]), stored(spark, PRODUCTS.table)
    )
    items = staged(spark, ITEMS, [item(1, 2, quantity=3), item(2, 3, quantity=None)])

    looked_up = with_as_of_lookup(items, ITEMS.key, PRODUCT, versions)

    assert rows_of(looked_up, "order_item_id", "unit_price", "line_amount") == [
        (1, Decimal("2.5000000000"), Decimal("7.5000000000")),
        (2, Decimal("2.5000000000"), None),
    ]


def test_a_status_takes_its_key_and_an_unknown_or_missing_one_the_unknown_status(
    spark: SparkSession,
) -> None:
    orders = staged(
        spark,
        ORDERS,
        [order(1, 1, "PENDING"), order(2, 2, "COMPLETED"), order(3, 3, "LOST"), order(4, 4, None)],
    )

    looked_up = with_key_lookups(orders, ORDERS.lookups, seeded(spark))

    assert rows_of(looked_up, "order_id", "status_key", "is_open") == [
        (1, 1, True),
        (2, 4, False),
        (3, 5, True),
        (4, 5, True),
    ]


def test_a_date_takes_its_calendar_key_and_a_date_outside_the_calendar_none(
    spark: SparkSession,
) -> None:
    orders = staged(
        spark,
        ORDERS,
        [
            order(1, 1, delivery_date=date(2026, 11, 20)),
            order(2, 2, delivery_date=date(1999, 1, 1)),
        ],
    )

    looked_up = with_key_lookups(orders, ORDERS.lookups, seeded(spark))

    assert rows_of(looked_up, "order_id", "order_date_key", "delivery_date_key") == [
        (1, 20261101, 20261120),
        (2, 20261101, None),
    ]
