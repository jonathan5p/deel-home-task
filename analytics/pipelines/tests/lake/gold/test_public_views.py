from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    customer,
    item,
    order,
    product,
    rows_of,
)
from analytics.pipelines.tests.lake.gold.public_setup import TABLES, VIEWS, LakeGold
from pipelines.lake.gold.public import public_columns

FIRST = date(2026, 11, 20)
SECOND = date(2026, 11, 21)
THIRD = date(2026, 11, 22)
FOURTH = date(2026, 11, 23)


@pytest.fixture
def gold(spark: SparkSession, iceberg_catalog: str) -> LakeGold:
    return LakeGold(spark, iceberg_catalog)


def customer_at(gold: LakeGold, orders: list[tuple[int, int | None, int]]) -> list[object]:
    """The customer name each order change sees, for (order, customer, minute) changes."""
    changes = [order(o, o, customer_id=c, minute=m) for o, c, m in orders]
    gold.load("fact_order_history", changes)
    return [
        name for _, name in rows_of(gold.view("fact_order_history"), "order_id", "customer_name")
    ]


def test_every_view_returns_its_public_columns(gold: LakeGold) -> None:
    for name, view in VIEWS.items():
        expected = [column.name for column in public_columns(TABLES[name], view.hidden)]

        assert gold.view(name).columns == expected, name


def test_an_order_change_sees_the_customer_version_valid_when_it_was_committed(
    gold: LakeGold,
) -> None:
    gold.load("dim_customer", [customer(1, 10, "Ada"), customer(1, 20, "Grace")])

    names = customer_at(gold, [(1, 1, 15), (2, 1, 20), (3, 1, 25), (4, 2, 25), (5, None, 25)])

    assert names == ["Ada", "Grace", "Grace", None, None]


def test_a_change_older_than_the_first_version_sees_the_first_version(gold: LakeGold) -> None:
    gold.load("dim_customer", [customer(1, 10, "Ada"), customer(1, 20, "Grace")])

    assert customer_at(gold, [(1, 1, 5)]) == ["Ada"]


def test_a_first_version_that_is_a_delete_covers_no_earlier_change(gold: LakeGold) -> None:
    gold.load("dim_customer", [customer(1, 10, "Ada", deleted=True), customer(1, 20, "Grace")])

    assert customer_at(gold, [(1, 1, 5), (2, 1, 25)]) == [None, "Grace"]


def test_a_change_after_a_delete_sees_no_version(gold: LakeGold) -> None:
    gold.load("dim_customer", [customer(1, 10, "Ada"), customer(1, 20, "Ada", deleted=True)])

    assert customer_at(gold, [(1, 1, 15), (2, 1, 25)]) == ["Ada", None]


def test_a_folded_version_is_never_seen(gold: LakeGold) -> None:
    gold.load("dim_customer", [customer(1, 10, "Ada"), customer(1, 20, "Ada")])

    gold.load("fact_order_history", [order(1, 1, minute=25)])

    ((key,),) = rows_of(gold.view("fact_order_history"), "customer_key")
    versions = rows_of(gold.view("dim_customer"), "customer_key")
    assert versions == [(key,)]


def test_when_clock_skew_overlaps_two_versions_the_one_committed_last_wins(
    gold: LakeGold,
) -> None:
    gold.load(
        "dim_customer",
        [
            customer(1, 10, "Ada", minute=100),
            customer(1, 20, "Grace", minute=110),
            customer(1, 30, "Hedy", minute=105),
        ],
    )

    assert customer_at(gold, [(1, 1, 107)]) == ["Hedy"]


def test_a_dimension_version_arriving_after_its_facts_is_seen_by_them(gold: LakeGold) -> None:
    gold.load("fact_order_history", [order(1, 1, minute=5)])
    assert rows_of(gold.view("fact_order_history"), "customer_name") == [(None,)]

    gold.load("dim_customer", [customer(1, 10, "Ada")])

    assert rows_of(gold.view("fact_order_history"), "customer_name") == [("Ada",)]


def test_an_item_change_sees_the_price_of_its_product_version_and_its_line_amount(
    gold: LakeGold,
) -> None:
    gold.load("dim_product", [product(1, 1, "2.5"), product(1, 10, "4.0")])

    gold.load(
        "fact_order_item_history",
        [item(1, 2, quantity=3), item(2, 3, quantity=None), item(3, 11, quantity=2)],
    )

    assert rows_of(
        gold.view("fact_order_item_history"), "order_item_id", "unit_price", "line_amount"
    ) == [
        (1, Decimal("2.5000000000"), Decimal("7.5000000000")),
        (2, Decimal("2.5000000000"), None),
        (3, Decimal("4.0000000000"), Decimal("8.0000000000")),
    ]


def test_the_history_views_leave_out_repeated_changes(gold: LakeGold) -> None:
    gold.load("fact_order_history", [order(1, 1), order(1, 2), order(1, 3, "COMPLETED")])

    assert rows_of(gold.view("fact_order_history"), "order_id", "status_key", "_is_current") == [
        (1, 1, False),
        (1, 4, True),
    ]


def load_open_orders(gold: LakeGold) -> None:
    """Orders by delivery date and customer: three dates tie for the second place, a completed
    and a deleted order count nowhere."""
    gold.load("dim_customer", [customer(1, 1, "Ada"), customer(2, 2, "Grace")])
    gold.load(
        "fact_order_history",
        [
            order(1, 10, customer_id=1, delivery_date=FIRST),
            order(2, 11, "PROCESSING", customer_id=1, delivery_date=FIRST),
            order(3, 12, customer_id=2, delivery_date=SECOND),
            order(4, 13, customer_id=3, delivery_date=THIRD),
            order(5, 14, customer_id=4, delivery_date=FOURTH),
            order(6, 15, "COMPLETED", customer_id=2, delivery_date=SECOND),
            order(7, 16, customer_id=2, delivery_date=SECOND),
            order(7, 17, customer_id=2, delivery_date=SECOND, deleted=True),
        ],
    )
    gold.load(
        "fact_order_item_history",
        [
            item(1, 20, order_id=1, product_id=10, quantity=2),
            item(2, 21, order_id=2, product_id=10, quantity=3),
            item(3, 22, order_id=6, product_id=10, quantity=5),
            item(4, 23, order_id=3, product_id=20, quantity=None),
            item(5, 24, order_id=3, product_id=20, quantity=1),
            item(5, 25, order_id=3, product_id=20, quantity=1, deleted=True),
        ],
    )


def test_open_orders_are_counted_by_delivery_date_and_status(gold: LakeGold) -> None:
    load_open_orders(gold)

    assert rows_of(
        gold.view("open_orders_by_delivery_date_and_status"),
        "delivery_date",
        "status_code",
        "open_orders",
    ) == [
        (FIRST, "PENDING", 1),
        (FIRST, "PROCESSING", 1),
        (SECOND, "PENDING", 1),
        (THIRD, "PENDING", 1),
        (FOURTH, "PENDING", 1),
    ]


def test_the_top_delivery_dates_keep_every_date_tied_for_a_place(gold: LakeGold) -> None:
    load_open_orders(gold)

    assert rows_of(
        gold.view("top_delivery_dates_by_open_orders"), "delivery_date", "open_orders"
    ) == [
        (FIRST, 2),
        (SECOND, 1),
        (THIRD, 1),
        (FOURTH, 1),
    ]


def test_pending_items_are_the_live_items_of_open_orders_by_product(gold: LakeGold) -> None:
    load_open_orders(gold)

    assert rows_of(
        gold.view("pending_items_by_product"), "product_id", "pending_items", "pending_quantity"
    ) == [(10, 2, 5), (20, 1, None)]


def test_the_top_customers_are_named_by_their_current_version_ties_included(
    gold: LakeGold,
) -> None:
    load_open_orders(gold)
    gold.load("dim_customer", [customer(1, 30, "Ada Lovelace")])

    assert rows_of(
        gold.view("top_customers_by_pending_orders"),
        "customer_id",
        "customer_name",
        "pending_orders",
    ) == [(1, "Ada Lovelace", 2), (2, "Grace", 1), (3, None, 1), (4, None, 1)]
