from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.gold.rebuild_setup import (
    DATABASE,
    ENTITIES,
    LATER,
    silver_tables,
    write_silver_tables,
)
from analytics.pipelines.tests.gold.silver_setup import CHANGED_AT, RecordingSinks
from pipelines.gold.rebuild import iso_time, read_silver_table, run, to_time, until
from pipelines.gold.sink import BatchWriter, GoldLoad
from pipelines.settings import GoldSettings

SETTINGS = GoldSettings.from_env(DATABASE)


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


def test_dimensions_are_merged_before_facts_each_in_one_batch_then_retention_runs(
    spark: SparkSession, silver: Path
) -> None:
    log: list[str] = []
    sinks = RecordingSinks()

    def sink(load: GoldLoad) -> BatchWriter:
        write = sinks(load)

        def logged(batch: DataFrame, batch_id: int) -> None:
            log.append(f"merge {load.name}")
            write(batch, batch_id)

        return logged

    def retain() -> list[str]:
        log.append("retain")
        return ["gold_internal.fact_order_history_2023_09"]

    dropped = run(
        spark,
        SETTINGS,
        to_time=None,
        retain=retain,
        read=silver_tables(silver, ENTITIES, log),
        sink=sink,
    )

    assert log == [
        "read catalog.silver.customers",
        "merge dim_customer",
        "read catalog.silver.products",
        "merge dim_product",
        "read catalog.silver.orders",
        "merge fact_order_history",
        "read catalog.silver.order_items",
        "merge fact_order_item_history",
        "retain",
    ]
    assert dropped == ["gold_internal.fact_order_history_2023_09"]
    assert {name: len(batches) for name, batches in sinks.batches.items()} == dict.fromkeys(
        ["dim_customer", "dim_product", "fact_order_history", "fact_order_item_history"], 1
    )
    assert sorted(row._commit_sequence for row in sinks.batches["dim_product"][0]) == [20, 90]


def test_a_cutoff_leaves_later_changes_out_of_every_load(spark: SparkSession, silver: Path) -> None:
    sinks = RecordingSinks()

    run(
        spark,
        SETTINGS,
        to_time=LATER.replace(tzinfo=UTC) - timedelta(minutes=1),
        retain=list,
        read=silver_tables(silver, ENTITIES, []),
        sink=sinks,
    )

    assert [row._commit_sequence for row in sinks.batches["dim_product"][0]] == [20]
    assert [row._commit_sequence for row in sinks.batches["fact_order_history"][0]] == [50]


def test_a_failed_load_stops_the_rebuild_before_retention(
    spark: SparkSession, silver: Path
) -> None:
    retained: list[str] = []

    def failing(load: GoldLoad) -> BatchWriter:
        def write(batch: DataFrame, batch_id: int) -> None:
            raise RuntimeError(f"cannot merge {load.name}")

        return write

    def retain() -> list[str]:
        retained.append("retain")
        return []

    with pytest.raises(RuntimeError, match="cannot merge dim_customer"):
        run(
            spark,
            SETTINGS,
            to_time=None,
            retain=retain,
            read=silver_tables(silver, ENTITIES, []),
            sink=failing,
        )

    assert retained == []


def test_a_silver_table_is_read_in_batch_from_the_catalog(
    spark: SparkSession, silver: Path, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.silver.orders"
    spark.read.parquet(str(silver / "orders")).writeTo(table).create()

    orders = read_silver_table(spark, table)

    assert not orders.isStreaming
    assert sorted(row._commit_sequence for row in orders.collect()) == [50, 95]
