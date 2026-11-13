import pytest

pytest.importorskip("pyspark")

import re
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.views.views_setup import (
    GoldLake,
    at,
    current_order,
    customer_version,
    order_change,
)
from analytics.pipelines.tests.lake.shared.tables_setup import stored_description
from analytics.pipelines.tests.lake.shared.views_setup import register_view_catalog, stored_views
from pipelines.lake.gold.ddl import render
from pipelines.lake.gold.views.views import (
    GoldLakeViews,
    MissingGoldTablesError,
    drop_gold_views,
    refresh_gold_views,
)
from pipelines.lake.shared.types import SPARK_TYPES
from pipelines.settings import LakeSettings

LAKE = LakeSettings(root="unused", checkpoint_root="unused", catalog="lake")
GOLD_VIEWS = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "dim_order_status",
    "fact_order_history",
    "fact_order_item_history",
    "current_orders",
    "open_orders",
    "current_order_items",
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
    "data_freshness",
]


def postgres_views() -> dict[str, list[str]]:
    """The columns of each public PostgreSQL view, from the comments of the rendered DDL."""
    columns: dict[str, list[str]] = {}
    for view, column in re.findall(r"COMMENT ON COLUMN gold\.(\w+)\.(\w+) IS", render()):
        columns.setdefault(view, []).append(column)
    return columns


def test_there_is_one_gold_view_per_public_postgres_view() -> None:
    views = GoldLakeViews(LAKE)

    assert list(views.definitions()) == [f"lake.gold.{name}" for name in GOLD_VIEWS]
    assert set(GOLD_VIEWS) == set(postgres_views())


def test_each_view_has_the_columns_of_its_postgres_view_surrogate_keys_included() -> None:
    lake = {
        view.rpartition(".")[2]: list(definition.columns)
        for view, definition in GoldLakeViews(LAKE).definitions().items()
    }

    assert lake == postgres_views()
    assert "customer_key" in lake["dim_customer"]
    assert "product_key" in lake["fact_order_item_history"]


def test_a_view_reads_its_gold_internal_table_with_the_postgres_filter() -> None:
    definitions = GoldLakeViews(LAKE).definitions()

    assert definitions["lake.gold.open_orders"].query.endswith(
        "FROM lake.gold_internal.current_orders\nWHERE is_open AND NOT _is_deleted"
    )
    assert definitions["lake.gold.dim_customer"].query.endswith(
        "FROM lake.gold_internal.dim_customer\nWHERE NOT _is_folded AND NOT _is_deleted"
    )
    assert definitions["lake.gold.pending_items_by_product"].query == (
        "SELECT\n    product_id,\n    pending_items,\n    pending_quantity\n"
        "FROM lake.gold_internal.pending_items_by_product"
    )


def test_the_views_read_only_gold_internal_tables() -> None:
    assert GoldLakeViews(LAKE).sources() == [
        f"lake.gold_internal.{name}"
        for name in (
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_order_status",
            "fact_order_history",
            "fact_order_item_history",
            "current_orders",
            "current_order_items",
            "open_orders_by_delivery_date_and_status",
            "top_delivery_dates_by_open_orders",
            "pending_items_by_product",
            "top_customers_by_pending_orders",
        )
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


def test_a_dimension_view_leaves_out_folded_and_deleted_versions(
    empty_gold_lake: GoldLake,
) -> None:
    empty_gold_lake.append(
        "dim_customer",
        customer_version(1, 10),
        customer_version(1, 20, folded=True),
        customer_version(1, 30, deleted=True),
        customer_version(1, 40, current=True),
    )

    assert empty_gold_lake.rows("dim_customer", "customer_key, _valid_from, _is_current") == [
        (1010, at(10), False),
        (1040, at(40), True),
    ]


def test_a_history_view_leaves_out_folded_changes(empty_gold_lake: GoldLake) -> None:
    empty_gold_lake.append(
        "fact_order_history", order_change(7, 10), order_change(7, 20, folded=True)
    )

    assert empty_gold_lake.rows("fact_order_history", "order_id, _changed_at") == [(7, at(10))]


def test_current_orders_leave_out_deletes_and_open_orders_the_closed_ones(
    empty_gold_lake: GoldLake,
) -> None:
    empty_gold_lake.append(
        "current_orders",
        current_order(1, 10),
        current_order(2, 10, is_open=False, status_key=4),
        current_order(3, 10, deleted=True),
    )

    assert empty_gold_lake.rows("current_orders", "order_id, status_key") == [(1, 1), (2, 4)]
    assert empty_gold_lake.rows("open_orders", "order_id") == [(1,)]


def test_a_business_question_view_returns_its_report_table(empty_gold_lake: GoldLake) -> None:
    empty_gold_lake.append(
        "top_delivery_dates_by_open_orders",
        {"delivery_date": date(2026, 11, 5), "open_orders": 3},
    )

    assert empty_gold_lake.rows(
        "top_delivery_dates_by_open_orders", "delivery_date, open_orders"
    ) == [(date(2026, 11, 5), 3)]


def test_freshness_is_the_newest_change_time_of_each_entity(empty_gold_lake: GoldLake) -> None:
    empty_gold_lake.append("dim_customer", customer_version(1, 10), customer_version(1, 30))
    empty_gold_lake.append("current_orders", current_order(1, 20))

    freshness = empty_gold_lake.spark.table(empty_gold_lake.view("data_freshness"))

    assert dict(tuple(row) for row in freshness.collect()) == {
        "customers": at(30),
        "products": None,
        "orders": at(20),
        "order_items": None,
    }


def test_the_views_wait_for_the_gold_tables(spark: SparkSession, tmp_path: Path) -> None:
    lake = LakeSettings(
        root="unused", checkpoint_root="unused", catalog=register_view_catalog(spark, tmp_path)
    )

    with pytest.raises(MissingGoldTablesError, match=r"gold_internal\.dim_customer"):
        refresh_gold_views(spark, GoldLakeViews(lake))


def test_dropping_the_views_drops_each_and_can_run_again(empty_gold_lake: GoldLake) -> None:
    spark = empty_gold_lake.spark

    drop_gold_views(spark, empty_gold_lake.views)
    drop_gold_views(spark, empty_gold_lake.views)

    namespace = empty_gold_lake.views.namespace
    assert stored_views(spark, namespace) == []
    assert spark.catalog.tableExists(empty_gold_lake.lake.gold_table("dim_customer"))
