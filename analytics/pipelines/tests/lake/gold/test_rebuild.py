from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.rebuild_setup import (
    DATABASE,
    ENTITIES,
    LATER,
    RecordingWriter,
    gold_in,
    silver_tables,
    write_silver_tables,
)
from analytics.pipelines.tests.lake.gold.silver_setup import CHANGED_AT
from analytics.pipelines.tests.lake.gold.views.views_setup import current_order
from analytics.pipelines.tests.lake.shared.views_setup import stored_views
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.job import gold_writer
from pipelines.lake.gold.lake import described_schema
from pipelines.lake.gold.rebuild import (
    iso_time,
    read_silver_table,
    recreated,
    run,
    to_time,
    until,
)
from pipelines.lake.gold.views.views import GoldLakeViews
from pipelines.settings import GoldSettings

SETTINGS = GoldSettings.from_env(DATABASE)
STORED_ORDERS = STORED["current_orders"]


@pytest.fixture
def silver(spark: SparkSession, tmp_path: Path) -> Path:
    root = tmp_path / "silver"
    write_silver_tables(spark, root)
    return root


def test_without_a_cutoff_the_rebuild_takes_everything() -> None:
    assert to_time([]) is None


def test_a_cutoff_without_an_offset_is_utc() -> None:
    cutoff = to_time(["--to-time", "2026-11-08T14:21:00"])

    assert cutoff == datetime(2026, 11, 8, 14, 21, tzinfo=UTC)


def test_a_cutoff_with_an_offset_is_converted_to_utc() -> None:
    cutoff = iso_time("2026-11-08T16:21:00+02:00")

    assert cutoff == datetime(2026, 11, 8, 14, 21, tzinfo=UTC)
    assert cutoff.tzinfo is UTC


def test_a_cutoff_that_is_not_iso_8601_is_refused() -> None:
    with pytest.raises(SystemExit):
        to_time(["--to-time", "yesterday"])


def test_a_cutoff_keeps_the_changes_committed_at_or_before_it(
    spark: SparkSession, silver: Path
) -> None:
    orders = spark.read.parquet(str(silver / "orders"))
    cutoff = CHANGED_AT.replace(tzinfo=UTC)

    kept = until(orders, cutoff).collect()

    assert [row._commit_sequence for row in kept] == [50]
    assert until(orders, None).count() == 2


def rebuild(
    spark: SparkSession,
    silver: Path,
    writer: RecordingWriter,
    log: list[str],
    *,
    to_time: datetime | None = None,
    retain: Callable[[], list[str]] = list,
) -> list[str]:
    return run(
        spark,
        SETTINGS,
        to_time=to_time,
        writer=writer,
        recreate=lambda: log.append("recreate"),
        copy=lambda: log.append("copy"),
        retain=retain,
        read=silver_tables(silver, ENTITIES, log),
    )


def test_the_tables_are_recreated_then_written_dimensions_first_then_copied_and_retained(
    spark: SparkSession, silver: Path
) -> None:
    log: list[str] = []
    writer = RecordingWriter(log)

    def retain() -> list[str]:
        log.append("retain")
        return ["gold_internal.fact_order_history_2023_09"]

    dropped = rebuild(spark, silver, writer, log, retain=retain)

    assert log == [
        "recreate",
        "read catalog.silver.customers",
        "write dim_customer",
        "read catalog.silver.products",
        "write dim_product",
        "read catalog.silver.orders",
        "write fact_order_history",
        "read catalog.silver.order_items",
        "write fact_order_item_history",
        "copy",
        "retain",
    ]
    assert dropped == ["gold_internal.fact_order_history_2023_09"]
    assert sorted(row._commit_sequence for row in writer.batches["dim_product"]) == [20, 90]


def test_a_cutoff_leaves_later_changes_out_of_every_load(spark: SparkSession, silver: Path) -> None:
    writer = RecordingWriter([])

    rebuild(spark, silver, writer, [], to_time=LATER.replace(tzinfo=UTC) - timedelta(minutes=1))

    assert [row._commit_sequence for row in writer.batches["dim_product"]] == [20]
    assert [row._commit_sequence for row in writer.batches["fact_order_history"]] == [50]


def test_a_failed_load_stops_the_rebuild_before_the_copy(spark: SparkSession, silver: Path) -> None:
    log: list[str] = []

    with pytest.raises(RuntimeError, match="cannot write dim_customer"):
        rebuild(spark, silver, RecordingWriter(log, fail=True), log)

    assert "copy" not in log


def test_a_silver_table_is_read_in_batch_from_the_catalog(
    spark: SparkSession, silver: Path, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.silver.orders"
    spark.read.parquet(str(silver / "orders")).writeTo(table).create()

    orders = read_silver_table(spark, table)

    assert not orders.isStreaming
    assert sorted(row._commit_sequence for row in orders.collect()) == [50, 95]


def test_recreating_empties_the_gold_tables_seeds_them_and_creates_the_views(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = gold_in(spark, tmp_path)
    writer, lake = gold_writer(spark, settings, serve=False)
    lake.ensure()
    orders = STORED_ORDERS
    row = current_order(1, 10)
    spark.createDataFrame(
        [tuple(row.get(name) for name in orders.columns)], described_schema(orders.table)
    ).writeTo(settings.stored_table(orders.name)).append()
    views = GoldLakeViews(settings.lake)

    recreated(spark, writer, lake, views)()

    assert lake.read("current_orders").isEmpty()
    assert lake.read("dim_order_status").count() == 5
    shown = stored_views(spark, views.namespace)
    assert {f"{views.namespace}.{name}" for name in shown} == set(views.definitions())
