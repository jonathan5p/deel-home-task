import pytest

pytest.importorskip("pyspark")

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.gold.lake.views_setup import order
from analytics.pipelines.tests.gold.quality_setup import (
    FAKE_KEY_OFFSET,
    SETTINGS,
    FakeLake,
    base_silver,
    batch,
    batches,
    by_view,
    samples,
)
from pipelines.gold.quality import (
    Bounds,
    UnexpectedSourcesError,
    aligned,
    check_gold,
    compare,
    differences,
    plain,
    sample,
    snapshot_choices,
    snapshot_id,
)
from pipelines.lake.checkpoints import CommittedBatch

EVERY_ENTITY = ("customers", "products", "orders", "order_items")
AT_FIRST = dict.fromkeys(EVERY_ENTITY, 1)
COMPARED = 13


def committed_at(lake: FakeLake, snapshots: dict[str, int]) -> None:
    lake.committed = batches(snapshots)


def test_the_choices_start_with_the_committed_snapshots() -> None:
    bounds = {"orders": Bounds(1, 2), "customers": Bounds(5, None), "products": Bounds(7, 7)}

    assert snapshot_choices(bounds) == [
        {"orders": 1, "customers": 5, "products": 7},
        {"orders": 2, "customers": 5, "products": 7},
    ]


def test_a_gold_stream_reads_exactly_one_silver_table() -> None:
    assert snapshot_id("gold/orders", batch(3)) == 3
    two_sources = CommittedBatch("query", 1, ({"snapshot_id": 3}, {"snapshot_id": 4}))
    with pytest.raises(UnexpectedSourcesError, match="2 sources"):
        snapshot_id("gold/orders", two_sources)


def test_compare_casts_postgres_to_the_lake_types(spark: SparkSession) -> None:
    lake = spark.createDataFrame([(1, "1.5")], "id long, price string").select(
        "id", F.col("price").cast("decimal(38,10)").alias("price")
    )
    postgres = spark.createDataFrame([(1, "1.500")], "id long, price string").select(
        "id", F.col("price").cast("decimal(38,18)").alias("price")
    )

    found = compare(lake, postgres)

    assert (found.lake_rows, found.lake_only, found.postgres_only, found.passed) == (1, 0, 0, True)


def test_a_lake_that_matches_postgres_passes_every_view(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    lake.add_snapshot(1, base_silver())
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST)

    checks = check_gold(spark, SETTINGS, lake.readers())

    assert len(checks) == COMPARED
    assert [result.subject for result in checks if not result.passed] == []
    orders = by_view(checks)["fact_order_history"]
    assert orders.metrics == {
        "lake_rows": 2,
        "postgres_rows": 2,
        "lake_only": 0,
        "postgres_only": 0,
        "stale": 0,
    }
    assert orders.details["snapshots"] == AT_FIRST


def test_rows_silver_holds_after_the_committed_batch_are_not_compared(
    spark: SparkSession,
) -> None:
    lake = FakeLake(spark)
    silver = base_silver()
    lake.add_snapshot(1, silver)
    lake.add_snapshot(2, silver | {"orders": [*silver["orders"], order(701, 50, "COMPLETED")]})
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST)

    checks = lake.check().run()

    assert all(check.passed for check in checks)


def test_a_batch_postgres_holds_before_its_checkpoint_commits_it_passes(
    spark: SparkSession,
) -> None:
    lake = FakeLake(spark)
    silver = base_silver()
    lake.add_snapshot(1, silver)
    lake.add_snapshot(2, silver | {"orders": [*silver["orders"], order(701, 50, "COMPLETED")]})
    committed_at(lake, AT_FIRST)
    lake.planned = batches({"orders": 2})
    lake.mirror_postgres(AT_FIRST | {"orders": 2})

    checks = by_view(lake.check().run())

    assert all(check.passed for check in checks.values())
    assert checks["current_orders"].details["snapshots"] == AT_FIRST | {"orders": 2}


def test_a_row_postgres_lacks_fails_with_a_sample_of_it(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    lake.add_snapshot(1, base_silver())
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST)
    items = "gold.fact_order_item_history"
    lake.postgres[items] = lake.postgres[items].limit(0)

    history = by_view(lake.check().run())["fact_order_item_history"]

    assert not history.passed
    assert (history.metrics["lake_only"], history.metrics["postgres_only"]) == (1, 0)
    (sample,) = samples(history, "lake_only")
    assert (sample["order_item_id"], sample["unit_price"]) == (801, "1.5000000000")


def test_a_fact_pointing_at_another_dimension_version_fails(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    lake.add_snapshot(1, base_silver())
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST)
    first_version = 10 + FAKE_KEY_OFFSET
    facts = lake.postgres["gold.fact_order_history"]
    lake.postgres["gold.fact_order_history"] = facts.withColumn(
        "customer_key", F.lit(first_version).cast("long")
    )

    checks = by_view(lake.check().run())

    assert not checks["fact_order_history"].passed
    assert checks["dim_customer"].passed


def test_history_is_compared_from_the_first_month_postgres_keeps(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    january, march = datetime(2026, 1, 10), datetime(2026, 3, 1)
    silver = base_silver()
    dropped = order(701, 25, "PENDING", op="create", changed_at=january)
    lake.add_snapshot(1, silver | {"orders": [dropped, *silver["orders"]]})
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST, {"orders": march})

    history = by_view(lake.check().run())["fact_order_history"]
    unbounded = lake.check().lake_rows(AT_FIRST, {})["fact_order_history"]

    assert history.passed
    assert history.details["history_from"] == march.isoformat()
    assert january in [row["_changed_at"] for row in unbounded.collect()]


def test_a_stale_lake_view_fails(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    lake.add_snapshot(1, base_silver())
    committed_at(lake, AT_FIRST)
    lake.mirror_postgres(AT_FIRST)
    lake.stale = {"lake.gold.dim_customer"}

    customers = by_view(lake.check().run())["dim_customer"]

    assert not customers.passed
    assert customers.metrics["stale"] == 1


def test_nothing_is_checked_before_every_gold_stream_has_committed(spark: SparkSession) -> None:
    lake = FakeLake(spark)
    lake.add_snapshot(1, base_silver())
    committed_at(lake, {"customers": 1, "products": 1, "orders": 1})

    checks = lake.check().run()

    assert len(checks) == COMPARED
    assert not any(check.passed for check in checks)
    assert checks[0].description.startswith("Not checked")
    assert checks[0].metrics == dict.fromkeys(checks[0].metrics)


def test_aligned_takes_the_columns_and_types_of_the_schema(spark: SparkSession) -> None:
    schema = spark.createDataFrame([], "id long, price decimal(38,10)").schema
    rows = spark.createDataFrame([("1.5", 1, "x")], "price string, id int, extra string")

    result = aligned(rows, schema)

    assert result.schema == schema
    assert result.collect()[0].asDict() == {"id": 1, "price": Decimal("1.5000000000")}


def test_differences_compare_the_rows_as_multisets(spark: SparkSession) -> None:
    expected = spark.createDataFrame([(1,), (1,), (2,)], "id long")
    actual = spark.createDataFrame([(1,), (3,)], "id long")

    found = differences(expected, actual)

    assert sorted(row.id for row in found.expected_only.collect()) == [1, 2]
    assert [row.id for row in found.actual_only.collect()] == [3]


def test_plain_values_fit_in_json() -> None:
    assert plain(datetime(2026, 11, 1, 10, 30)) == "2026-11-01T10:30:00"
    assert plain(date(2026, 11, 1)) == "2026-11-01"
    assert plain(Decimal("1.50")) == "1.50"
    assert [plain(value) for value in (None, True, 3, 1.5, "a")] == [None, True, 3, 1.5, "a"]


def test_a_sample_keeps_at_most_size_distinct_values_in_order(spark: SparkSession) -> None:
    rows = spark.createDataFrame([(3, "c"), (1, "a"), (1, "a"), (2, "b")], "id long, name string")

    assert sample(rows, ["id"], 2) == ({"id": 1}, {"id": 2})
    assert sample(rows, ["id", "name"], 5) == (
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    )
