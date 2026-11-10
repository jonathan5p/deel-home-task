from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.gold.quality_setup import (
    CUSTOMER_SNAPSHOT,
    CUSTOMER_VIEW,
    CUSTOMERS,
    ENV,
    PRODUCTS,
    SETTINGS,
    at,
    batch,
    checkpoints,
    customer_versions,
    order_change,
    order_changes,
    readers,
    snapshots,
    successive,
    unread_gold,
)
from analytics.pipelines.tests.gold.silver_setup import silver_schema, silver_version
from common.settings import AnalyticsDbSettings
from pipelines.gold.quality import (
    COMPARISONS,
    GOLD_MATCHES_SILVER,
    NOT_CHECKED,
    CommittedBatch,
    GoldComparison,
    GoldReaders,
    GoldShape,
    UnexpectedSourcesError,
    check_gold,
    check_view,
    gold_read_options,
    read_silver_snapshot,
    run_gold_checks,
    snapshot_bounds,
)

BY_VIEW = {comparison.view: comparison for comparison in COMPARISONS}


def test_a_dimension_view_compares_its_business_columns_and_silver_validity() -> None:
    comparison = BY_VIEW["dim_customer"]

    assert comparison == GoldComparison(
        view="dim_customer",
        stream="dim_customer",
        entity=comparison.entity,
        shape=GoldShape.VERSIONS,
        key=("customer_id", "_valid_from"),
        columns={
            "customer_id": "customer_id",
            "customer_name": "customer_name",
            "customer_address": "customer_address",
            "is_active": "is_active",
            "_valid_from": "_valid_from",
            "_valid_to": "_valid_to",
            "_is_current": "_is_current",
        },
    )
    assert comparison.entity.name == "customers"
    assert comparison.relation == "gold.dim_customer"


def test_a_renamed_dimension_column_is_compared_with_its_silver_column() -> None:
    assert BY_VIEW["dim_product"].columns["unit_price"] == "unity_price"


def test_a_history_view_compares_each_change_without_the_looked_up_keys() -> None:
    orders, items = BY_VIEW["fact_order_history"], BY_VIEW["fact_order_item_history"]

    assert (orders.stream, orders.shape, orders.key) == (
        "fact_order_history",
        GoldShape.CHANGES,
        ("order_id", "_changed_at"),
    )
    assert list(orders.columns) == [
        "order_id",
        "_changed_at",
        "_change_op",
        "customer_id",
        "_valid_to",
        "_is_current",
    ]
    assert list(items.columns) == [
        "order_item_id",
        "_changed_at",
        "_change_op",
        "order_id",
        "product_id",
        "quantity",
        "_valid_to",
        "_is_current",
    ]


def test_a_current_view_compares_its_business_columns_by_key_without_the_change_time() -> None:
    orders, items = BY_VIEW["current_orders"], BY_VIEW["current_order_items"]

    assert (orders.stream, orders.shape, orders.key) == (
        "fact_order_history",
        GoldShape.CURRENT,
        ("order_id",),
    )
    assert list(orders.columns) == ["order_id", "customer_id", "order_date", "delivery_date"]
    assert list(items.columns) == ["order_item_id", "order_id", "product_id", "quantity"]
    assert items.stream == "fact_order_item_history"


def test_every_public_table_view_is_compared_once() -> None:
    assert [comparison.view for comparison in COMPARISONS] == [
        "dim_customer",
        "dim_product",
        "fact_order_history",
        "current_orders",
        "fact_order_item_history",
        "current_order_items",
    ]


def test_the_bounds_are_the_committed_snapshot_and_a_newer_one() -> None:
    assert snapshot_bounds(batch(1), None) == (1,)
    assert snapshot_bounds(batch(1), batch(2, number=1)) == (1, 2)
    assert snapshot_bounds(batch(1), batch(1)) == (1,)


def test_a_batch_must_read_exactly_one_silver_table() -> None:
    two = CommittedBatch("query", 4, ({"snapshot_id": 1}, {"snapshot_id": 2}))

    with pytest.raises(UnexpectedSourcesError, match="batch 4 of query query has 2 sources"):
        snapshot_bounds(two, None)


def test_gold_is_read_through_jdbc_with_the_analytics_database_settings() -> None:
    settings = AnalyticsDbSettings(url="jdbc:postgresql://db/gold", user="u", password="p")

    assert gold_read_options(settings, "gold.dim_customer") == {
        "url": "jdbc:postgresql://db/gold",
        "user": "u",
        "password": "p",
        "driver": "org.postgresql.Driver",
        "dbtable": "gold.dim_customer",
    }


def test_a_view_matching_silver_at_the_committed_snapshot_passes_with_repeats_folded(
    spark: SparkSession,
) -> None:
    sources = readers(
        spark, {CUSTOMER_SNAPSHOT: customer_versions()}, {"dim_customer": CUSTOMER_VIEW}, batch(1)
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], sources)

    assert checked.result == GOLD_MATCHES_SILVER.result(
        "gold.dim_customer",
        passed=True,
        metrics={
            "silver_rows": 3,
            "gold_rows": 3,
            "missing": 0,
            "extra": 0,
            "different": 0,
            "snapshot_id": 1,
        },
    )
    assert checked.samples == {"missing": (), "extra": (), "different": ()}


def test_a_view_one_batch_ahead_passes_at_the_planned_snapshot(spark: SparkSession) -> None:
    earlier = [row for row in customer_versions() if row["_commit_sequence"] < 30]
    silver = {CUSTOMER_SNAPSHOT: earlier, ("catalog.silver.customers", 2): customer_versions()}
    sources = readers(spark, silver, {"dim_customer": CUSTOMER_VIEW}, batch(1), batch(2, 1))

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], sources)

    assert checked.result.passed
    assert checked.result.metrics["snapshot_id"] == 2


def test_a_batch_committed_while_gold_is_read_is_a_bound_too(spark: SparkSession) -> None:
    earlier = [row for row in customer_versions() if row["_commit_sequence"] < 30]
    silver = {CUSTOMER_SNAPSHOT: earlier, ("catalog.silver.customers", 2): customer_versions()}
    sources = readers(spark, silver, {"dim_customer": CUSTOMER_VIEW})
    moving = GoldReaders(
        committed=successive(batch(1), batch(2, 1)),
        planned=checkpoints({}),
        read_gold=sources.read_gold,
        read_snapshot=sources.read_snapshot,
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], moving)

    assert checked.result.passed
    assert checked.result.metrics["snapshot_id"] == 2


def test_a_view_matching_neither_bound_fails_against_the_committed_one(
    spark: SparkSession,
) -> None:
    earlier = [row for row in customer_versions() if row["_commit_sequence"] < 30]
    later = customer_versions()
    silver = {CUSTOMER_SNAPSHOT: earlier, ("catalog.silver.customers", 2): later}
    sources = readers(spark, silver, {"dim_customer": CUSTOMER_VIEW[:1]}, batch(1), batch(2, 1))

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], sources)

    assert not checked.result.passed
    assert checked.result.metrics == {
        "silver_rows": 2,
        "gold_rows": 1,
        "missing": 1,
        "extra": 0,
        "different": 1,
        "snapshot_id": 1,
    }


def test_missing_extra_and_different_rows_are_counted_with_sample_keys(
    spark: SparkSession,
) -> None:
    price = {"unity_price": Decimal("1.5300000000")}
    silver = [
        silver_version(PRODUCTS, 3, 10, product_name="Pen", _changed_at=at(1), **price),
        silver_version(PRODUCTS, 4, 11, product_name="Ink", _changed_at=at(2), **price),
        silver_version(PRODUCTS, 5, 12, product_name="Pad", _changed_at=at(3), **price),
    ]

    def product(key: int, name: str, unit_price: str, second: int) -> dict[str, object]:
        return {
            "product_key": key + 100,
            "product_id": key,
            "product_name": name,
            "unit_price": Decimal(unit_price),
            "_valid_from": at(second),
            "_is_current": True,
        }

    gold = [product(3, "Pen", "9.99", 1), product(5, "Pad", "1.53", 3), product(6, "Cap", "1", 4)]
    sources = readers(
        spark, {("catalog.silver.products", 1): silver}, {"dim_product": gold}, batch(1)
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_product"], sources)

    assert checked.result.metrics == {
        "silver_rows": 3,
        "gold_rows": 3,
        "missing": 1,
        "extra": 1,
        "different": 1,
        "snapshot_id": 1,
    }
    assert checked.samples == {
        "missing": ({"product_id": 4, "_valid_from": at(2).isoformat()},),
        "extra": ({"product_id": 6, "_valid_from": at(4).isoformat()},),
        "different": ({"product_id": 3, "_valid_from": at(1).isoformat()},),
    }


def test_samples_keep_at_most_the_sample_size_in_key_order(spark: SparkSession) -> None:
    silver = [
        silver_version(CUSTOMERS, key, key, customer_name="C", _changed_at=at(key))
        for key in (5, 3, 4)
    ]
    sources = readers(spark, {CUSTOMER_SNAPSHOT: silver}, {"dim_customer": []}, batch(1))

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], sources, sample_size=2)

    assert checked.result.metrics["missing"] == 3
    assert [sample["customer_id"] for sample in checked.samples["missing"]] == [3, 4]


def test_a_history_view_shows_a_delete_as_the_last_change_never_current(
    spark: SparkSession,
) -> None:
    gold = [
        order_change(70, 1, "update", valid_to=3),
        order_change(70, 3, "delete", valid_to=None),
        order_change(71, 4, "update", valid_to=None),
    ]
    sources = readers(
        spark,
        {("catalog.silver.orders", 1): order_changes()},
        {"fact_order_history": gold},
        batch(1),
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["fact_order_history"], sources)

    assert checked.result.passed, checked.samples
    assert checked.result.metrics["silver_rows"] == 3


def test_a_current_view_hides_deleted_rows_and_keeps_the_latest_change_time(
    spark: SparkSession,
) -> None:
    gold = [{"order_id": 71, "status_key": 1, "customer_id": 5, "_changed_at": at(5)}]
    sources = readers(
        spark, {("catalog.silver.orders", 1): order_changes()}, {"current_orders": gold}, batch(1)
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["current_orders"], sources)

    assert checked.result.passed, checked.samples
    assert checked.result.metrics["silver_rows"] == 1


def test_a_stream_without_a_committed_batch_is_reported_as_not_checked(
    spark: SparkSession,
) -> None:
    sources = GoldReaders(
        committed=checkpoints({}),
        planned=checkpoints({}),
        read_gold=unread_gold,
        read_snapshot=snapshots({}),
    )

    checked = check_view(spark, SETTINGS, BY_VIEW["dim_customer"], sources)

    assert checked.result == NOT_CHECKED.result(
        "gold.dim_customer",
        passed=False,
        metrics=dict.fromkeys(
            ["silver_rows", "gold_rows", "missing", "extra", "different", "snapshot_id"]
        ),
    )
    assert checked.samples == {}


def test_check_gold_checks_every_view_it_is_given(spark: SparkSession) -> None:
    sources = readers(
        spark, {CUSTOMER_SNAPSHOT: customer_versions()}, {"dim_customer": CUSTOMER_VIEW}, batch(1)
    )
    comparisons = [BY_VIEW["dim_customer"], BY_VIEW["dim_product"]]
    unchecked = GoldReaders(
        committed=checkpoints({"dim_customer": batch(1)}),
        planned=checkpoints({}),
        read_gold=sources.read_gold,
        read_snapshot=sources.read_snapshot,
    )

    checks = check_gold(spark, SETTINGS, unchecked, comparisons)

    assert [(check.result.subject, check.result.passed) for check in checks] == [
        ("gold.dim_customer", True),
        ("gold.dim_product", False),
    ]
    assert checks[1].result.description == NOT_CHECKED.description


def test_run_gold_checks_reports_every_view_from_the_environment(spark: SparkSession) -> None:
    results = run_gold_checks(
        spark,
        ENV,
        committed=checkpoints({}),
        planned=checkpoints({}),
        read_gold=unread_gold,
        read_snapshot=snapshots({}),
    )

    assert [result.subject for result in results] == [
        f"gold.{comparison.view}" for comparison in COMPARISONS
    ]
    assert not any(result.passed for result in results)


def test_run_gold_checks_needs_the_checkpoint_readers_wired(spark: SparkSession) -> None:
    with pytest.raises(NotImplementedError, match="shared quality job"):
        run_gold_checks(spark, ENV)


def test_a_silver_snapshot_is_read_as_of_its_snapshot_id(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.silver.customers"
    first, second = customer_versions()[:1], customer_versions()[1:2]
    spark.createDataFrame(first, silver_schema(CUSTOMERS)).writeTo(table).create()
    spark.createDataFrame(second, silver_schema(CUSTOMERS)).writeTo(table).append()
    ids = [
        row.snapshot_id
        for row in spark.table(f"{table}.snapshots").orderBy("committed_at").collect()
    ]

    rows = read_silver_snapshot(spark, table, ids[0]).collect()

    assert [row["_commit_sequence"] for row in rows] == [10]
