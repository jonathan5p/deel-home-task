import pytest

pytest.importorskip("pyspark")

import re
from datetime import date
from decimal import Decimal

from analytics.pipelines.tests.gold.lake.views_setup import (
    DELIVERY_DAY,
    GoldLake,
    at,
    customer,
    item,
    order,
    product,
)
from analytics.pipelines.tests.lake.tables_setup import stored_description
from pipelines.gold.ddl import render
from pipelines.gold.lake.views import GoldLakeViews
from pipelines.lake.types import SPARK_TYPES
from pipelines.settings import LakeSettings

PENDING, PROCESSING, COMPLETED, UNKNOWN = 1, 2, 4, 5
ORDER_DAY_KEY, DELIVERY_DAY_KEY = 20261101, 20261105
GOLD_VIEWS = [
    "dim_date",
    "dim_order_status",
    "dim_customer",
    "dim_product",
    "fact_order_history",
    "fact_order_item_history",
    "current_orders",
    "current_order_items",
    "open_orders",
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
    "data_freshness",
]


def test_every_gold_view_follows_the_views_it_reads() -> None:
    views = GoldLakeViews(LakeSettings(root="unused", checkpoint_root="unused", catalog="lake"))

    assert list(views.definitions()) == [f"lake.gold.{name}" for name in GOLD_VIEWS]


def test_each_view_has_the_columns_of_the_postgres_view_without_the_surrogate_keys() -> None:
    views = GoldLakeViews(LakeSettings(root="unused", checkpoint_root="unused", catalog="lake"))
    postgres: dict[str, list[str]] = {}
    for view, column in re.findall(r"COMMENT ON COLUMN gold\.(\w+)\.(\w+) IS", render()):
        postgres.setdefault(view, []).append(column)

    lake = {view.rpartition(".")[2]: list(d.columns) for view, d in views.definitions().items()}

    surrogate_keys = {"customer_key", "product_key"}
    assert lake == {
        view: [column for column in columns if column not in surrogate_keys]
        for view, columns in postgres.items()
    }


def test_the_views_have_the_public_columns_without_the_surrogate_keys() -> None:
    views = GoldLakeViews(LakeSettings(root="unused", checkpoint_root="unused", catalog="lake"))
    definitions = views.definitions()

    def columns(name: str) -> list[str]:
        return list(definitions[f"lake.gold.{name}"].columns)

    assert columns("dim_product") == [
        "product_id",
        "product_name",
        "barcode",
        "unit_price",
        "is_active",
        "_valid_from",
        "_valid_to",
        "_is_current",
    ]
    assert columns("fact_order_history") == [
        "order_id",
        "_changed_at",
        "_change_op",
        "status_key",
        "customer_id",
        "order_date_key",
        "delivery_date_key",
        "_valid_to",
        "_is_current",
    ]
    assert columns("fact_order_item_history") == [
        "order_item_id",
        "_changed_at",
        "_change_op",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_amount",
        "_valid_to",
        "_is_current",
    ]


def test_each_view_returns_its_columns_with_their_types(gold_lake: GoldLake) -> None:
    for view, definition in gold_lake.views.definitions().items():
        fields = gold_lake.spark.table(view).schema.fields

        assert [field.name for field in fields] == list(definition.columns), view
        assert [field.dataType for field in fields] == [
            SPARK_TYPES[column.type] for column in definition.columns.values()
        ], view


def test_the_views_store_the_public_descriptions_of_the_gold_schemas(
    gold_lake: GoldLake,
) -> None:
    for view, definition in gold_lake.views.definitions().items():
        stored = stored_description(gold_lake.spark, view)

        assert stored.comment == definition.comment
        assert stored.columns == {
            name: column.description for name, column in definition.columns.items()
        }


def test_refreshing_unchanged_views_replaces_none(gold_lake: GoldLake) -> None:
    assert gold_lake.refresh() == []


def test_a_dimension_has_one_row_per_version_with_its_validity_range(gold_lake: GoldLake) -> None:
    gold_lake.append("customers", customer(101, 10, "Ana", op="snapshot"), customer(101, 20, "Bo"))

    assert gold_lake.rows(
        "dim_customer",
        "customer_id, customer_name, _valid_from, _valid_to, _is_current",
        "customer_id",
        101,
    ) == [(101, "Ana", at(10), at(20), False), (101, "Bo", at(20), None, True)]


def test_a_dimension_leaves_out_unchanged_versions_and_deletes(gold_lake: GoldLake) -> None:
    gold_lake.append(
        "customers",
        customer(102, 10, "Cleo", op="create"),
        customer(102, 20, "Cleo", op="snapshot"),
        customer(103, 10, "Dan", op="create"),
        customer(103, 30, "Dan", op="delete", deleted=True),
    )

    assert gold_lake.rows(
        "dim_customer", "customer_id, _valid_from, _valid_to, _is_current", "customer_id", 102, 103
    ) == [(102, at(10), None, True), (103, at(10), at(30), False)]


def test_a_product_takes_its_unit_price_from_the_source_unity_price(gold_lake: GoldLake) -> None:
    gold_lake.append("products", product(104, 10, "1.53", op="create"))

    assert gold_lake.rows("dim_product", "product_id, unit_price", "product_id", 104) == [
        (104, Decimal("1.5300000000"))
    ]


def test_the_order_history_folds_unchanged_changes_and_ranges_the_others(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append(
        "orders",
        order(201, 10, "PENDING", op="create"),
        order(201, 20, "PENDING", op="snapshot"),
        order(201, 30, "COMPLETED"),
    )

    assert gold_lake.rows(
        "fact_order_history",
        "order_id, _changed_at, _change_op, status_key, _valid_to, _is_current",
        "order_id",
        201,
    ) == [
        (201, at(10), "create", PENDING, at(30), False),
        (201, at(30), "update", COMPLETED, None, True),
    ]


def test_a_delete_is_the_last_change_of_its_order_and_never_current(gold_lake: GoldLake) -> None:
    gold_lake.append(
        "orders",
        order(202, 10, "PENDING", op="create"),
        order(202, 20, "PENDING", op="delete", deleted=True),
    )

    assert gold_lake.rows(
        "fact_order_history", "order_id, _change_op, _valid_to, _is_current", "order_id", 202
    ) == [(202, "create", at(20), False), (202, "delete", None, False)]


def test_a_null_or_unexpected_status_has_the_unknown_key(gold_lake: GoldLake) -> None:
    gold_lake.append(
        "orders",
        order(203, 10, None, op="create"),
        order(204, 10, "ON HOLD", op="create"),
        order(205, 10, "PROCESSING", op="create"),
    )

    assert gold_lake.rows(
        "fact_order_history", "order_id, status_key", "order_id", 203, 204, 205
    ) == [
        (203, UNKNOWN),
        (204, UNKNOWN),
        (205, PROCESSING),
    ]


def test_a_date_key_is_null_for_a_null_date_or_one_outside_the_calendar(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append(
        "orders",
        order(206, 10, "PENDING", op="create"),
        order(207, 10, "PENDING", op="create", delivery_date=None),
        order(208, 10, "PENDING", op="create", delivery_date=date(2150, 1, 1)),
    )

    assert gold_lake.rows(
        "fact_order_history",
        "order_id, order_date_key, delivery_date_key",
        "order_id",
        206,
        207,
        208,
    ) == [
        (206, ORDER_DAY_KEY, DELIVERY_DAY_KEY),
        (207, ORDER_DAY_KEY, None),
        (208, ORDER_DAY_KEY, None),
    ]


def test_an_item_takes_the_price_of_the_product_version_valid_when_it_changed(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append("products", product(301, 10, "1.33", op="snapshot"), product(301, 30, "2.00"))
    gold_lake.append("order_items", item(401, 20, 301, 3), item(402, 40, 301, 2))

    assert gold_lake.rows(
        "fact_order_item_history",
        "order_item_id, unit_price, line_amount",
        "order_item_id",
        401,
        402,
    ) == [
        (401, Decimal("1.3300000000"), Decimal("3.9900000000")),
        (402, Decimal("2.0000000000"), Decimal("4.0000000000")),
    ]


def test_an_item_before_its_products_first_version_takes_that_version(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append("products", product(302, 50, "5.00", op="snapshot"))
    gold_lake.append("order_items", item(403, 45, 302, 1))

    assert gold_lake.rows(
        "fact_order_item_history", "order_item_id, unit_price", "order_item_id", 403
    ) == [(403, Decimal("5.0000000000"))]


def test_an_item_of_a_deleted_product_has_no_price(gold_lake: GoldLake) -> None:
    gold_lake.append(
        "products",
        product(303, 10, "3.00", op="create"),
        product(303, 20, "3.00", op="delete", deleted=True),
        product(304, 50, "4.00", op="delete", deleted=True),
    )
    gold_lake.append("order_items", item(404, 30, 303, 1), item(405, 40, 304, 1))

    assert gold_lake.rows(
        "fact_order_item_history",
        "order_item_id, unit_price, line_amount",
        "order_item_id",
        404,
        405,
    ) == [(404, None, None), (405, None, None)]


def test_when_clock_skew_overlaps_two_versions_the_one_committed_last_wins(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append(
        "products",
        product(305, 10, "1.00", op="create"),
        product(305, 20, "2.00"),
        product(305, 30, "3.00", changed_at=at(15)),
    )
    gold_lake.append("order_items", item(406, 40, 305, 1, changed_at=at(17)))

    assert gold_lake.rows(
        "fact_order_item_history", "order_item_id, unit_price", "order_item_id", 406
    ) == [(406, Decimal("3.0000000000"))]


def test_a_late_product_version_moves_the_items_it_covers_without_a_refresh(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append("products", product(306, 10, "1.00", op="create"), product(306, 30, "3.00"))
    gold_lake.append("order_items", item(407, 40, 306, 1, changed_at=at(25)))
    before = gold_lake.rows("fact_order_item_history", "unit_price", "order_item_id", 407)

    gold_lake.append("products", product(306, 20, "2.00"))

    after = gold_lake.rows("fact_order_item_history", "unit_price", "order_item_id", 407)
    assert (before, after) == ([(Decimal("1.0000000000"),)], [(Decimal("2.0000000000"),)])


def test_the_current_order_is_its_latest_change_even_when_an_older_one_arrives_later(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append("orders", order(501, 10, "PENDING", op="create"), order(501, 30, "COMPLETED"))
    gold_lake.append("orders", order(501, 20, "PROCESSING"))

    assert gold_lake.rows(
        "current_orders",
        "order_id, status_key, order_date, delivery_date, order_date_key, _changed_at",
        "order_id",
        501,
    ) == [(501, COMPLETED, date(2026, 11, 1), DELIVERY_DAY, ORDER_DAY_KEY, at(30))]


def test_an_unchanged_resnapshot_is_the_current_order_with_its_own_change_time(
    gold_lake: GoldLake,
) -> None:
    gold_lake.append(
        "orders", order(502, 10, "PENDING", op="create"), order(502, 20, "PENDING", op="snapshot")
    )

    assert gold_lake.rows("current_orders", "order_id, _changed_at", "order_id", 502) == [
        (502, at(20))
    ]


def test_deleted_orders_and_items_are_not_current(gold_lake: GoldLake) -> None:
    gold_lake.append(
        "orders",
        order(503, 10, "PENDING", op="create"),
        order(503, 20, "PENDING", op="delete", deleted=True),
    )
    gold_lake.append(
        "order_items",
        item(601, 10, 104, 2),
        item(601, 20, 104, 2, deleted=True),
        item(602, 10, 104, 5),
    )

    assert gold_lake.rows("current_orders", "order_id", "order_id", 503) == []
    assert gold_lake.rows(
        "current_order_items", "order_item_id, product_id, quantity", "order_item_id", 601, 602
    ) == [(602, 104, 5)]


def test_the_calendar_has_every_day_of_the_century_by_iso_weekday(gold_lake: GoldLake) -> None:
    calendar = gold_lake.spark.table(gold_lake.view("dim_date"))

    days = calendar.where("date_key IN (20000101, 20261107, 20261109, 20991231)").orderBy(
        "date_key"
    )
    assert calendar.count() == 36525
    assert [tuple(row) for row in days.collect()] == [
        (20000101, date(2000, 1, 1), 2000, 1, 1, 1, 6, True),
        (20261107, date(2026, 11, 7), 2026, 4, 11, 7, 6, True),
        (20261109, date(2026, 11, 9), 2026, 4, 11, 9, 1, False),
        (20991231, date(2099, 12, 31), 2099, 4, 12, 31, 4, False),
    ]


def test_the_statuses_are_seeded_with_only_completed_closed(gold_lake: GoldLake) -> None:
    statuses = gold_lake.spark.table(gold_lake.view("dim_order_status")).orderBy("status_key")

    assert [tuple(row) for row in statuses.collect()] == [
        (1, "PENDING", True),
        (2, "PROCESSING", True),
        (3, "REPROCESSING", True),
        (4, "COMPLETED", False),
        (5, "UNKNOWN", True),
    ]


def test_freshness_is_the_newest_change_time_of_each_entity(empty_gold_lake: GoldLake) -> None:
    empty_gold_lake.append("customers", customer(1, 10, "Ana"), customer(1, 30, "Bo"))
    empty_gold_lake.append("orders", order(1, 20, "PENDING"))

    freshness = empty_gold_lake.spark.table(empty_gold_lake.view("data_freshness"))

    assert dict(tuple(row) for row in freshness.collect()) == {
        "customers": at(30),
        "products": None,
        "orders": at(20),
        "order_items": None,
    }


def test_the_business_questions_read_the_open_orders(empty_gold_lake: GoldLake) -> None:
    gold = empty_gold_lake
    gold.append("customers", customer(1, 5, "Ana", op="create"))
    gold.append(
        "orders",
        order(701, 10, "PENDING", op="create"),
        order(702, 10, "COMPLETED", op="create"),
        order(703, 10, "PROCESSING", op="create", customer_id=2, delivery_date=date(2026, 11, 6)),
        order(704, 10, "PENDING", op="create"),
        order(704, 20, "PENDING", op="delete", deleted=True),
    )
    gold.append(
        "order_items",
        item(801, 10, 9, 2, order_id=701),
        item(802, 10, 9, 3, order_id=703),
        item(803, 10, 9, 5, order_id=702),
    )

    def rows(view: str) -> list[tuple[object, ...]]:
        frame = gold.spark.table(gold.view(view))
        return [tuple(row) for row in frame.orderBy(*frame.columns).collect()]

    assert [row[0] for row in rows("open_orders")] == [701, 703]
    assert rows("open_orders_by_delivery_date_and_status") == [
        (DELIVERY_DAY, "PENDING", 1),
        (date(2026, 11, 6), "PROCESSING", 1),
    ]
    assert rows("top_delivery_dates_by_open_orders") == [(DELIVERY_DAY, 1), (date(2026, 11, 6), 1)]
    assert rows("pending_items_by_product") == [(9, 2, 5)]
    assert rows("top_customers_by_pending_orders") == [(1, "Ana", 1), (2, None, 1)]
