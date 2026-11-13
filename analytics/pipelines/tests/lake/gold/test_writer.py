import json
import logging
from collections.abc import Sequence

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.metrics_setup import FakeJobs, Ticks
from analytics.pipelines.tests.lake.gold.writer_setup import (
    CONFLICT,
    ITEMS,
    OTHER,
    SEEDS,
    ConflictError,
    FakeBatches,
    FakeLake,
    FakeServing,
    items,
    seeds,
)
from pipelines.lake.gold.metrics import BATCH_PREFIX, LOGGER
from pipelines.lake.gold.serving import Copy
from pipelines.lake.gold.writer import GoldWriter, difference, is_commit_conflict


def writer(
    spark: SparkSession,
    log: list[str],
    planned: Sequence[tuple[str, DataFrame]] = (),
    *,
    conflicts: dict[str, int] | None = None,
    serving: FakeServing | None = None,
    max_retries: int = 3,
) -> tuple[GoldWriter, FakeLake]:
    lake = FakeLake(
        tables={
            SEEDS.name: seeds(spark),
            ITEMS.name: items(spark),
            OTHER.name: spark.createDataFrame([], "customer_id long"),
        },
        log=log,
        conflicts=conflicts or {},
    )
    batches = FakeBatches(list(planned), log)
    return GoldWriter(batches, lake, serving, max_retries=max_retries), lake


def test_a_batch_merges_each_table_once_then_copies_them_in_one_go(spark: SparkSession) -> None:
    log: list[str] = []
    planned = [(SEEDS.name, seeds(spark, (1, "PENDING", True))), (ITEMS.name, items(spark, (5, 2)))]
    gold, _ = writer(spark, log, planned, serving=FakeServing(log))

    gold.write_batch("fact_order_item_history", spark.range(1))

    assert log == [
        "stage fact_order_item_history",
        "compute fact_order_item_history",
        "lake write dim_order_status 1",
        "lake write current_order_items 1",
        "copy [dim_order_status upsert 1, current_order_items upsert 1]"
        " {'dim_order_status': 1, 'current_order_items': 1}",
    ]


def test_writes_that_change_nothing_are_neither_merged_nor_copied(spark: SparkSession) -> None:
    log: list[str] = []
    gold, _ = writer(spark, log, [(SEEDS.name, seeds(spark))], serving=FakeServing(log))

    gold.write_batch("fact_order_item_history", spark.range(1))

    assert log == ["stage fact_order_item_history", "compute fact_order_item_history"]


def test_a_commit_conflict_replays_the_batch_after_copying_what_the_failed_try_wrote(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    planned = [(SEEDS.name, seeds(spark, (1, "PENDING", True))), (ITEMS.name, items(spark, (5, 2)))]
    serving = FakeServing(log, {SEEDS.name: None, ITEMS.name: None})
    gold, _ = writer(spark, log, planned, conflicts={ITEMS.name: 1}, serving=serving)

    gold.write_batch("fact_order_item_history", spark.range(1))

    assert log == [
        "stage fact_order_item_history",
        "compute fact_order_item_history",
        "lake write dim_order_status 1",
        "lake write current_order_items 1",
        "copy [dim_order_status upsert 1] {'dim_order_status': 1}",
        "compute fact_order_item_history",
        "lake write dim_order_status 1",
        "lake write current_order_items 1",
        "copy [dim_order_status upsert 1, current_order_items upsert 1]"
        " {'dim_order_status': 2, 'current_order_items': 1}",
    ]


def test_a_batch_catches_up_only_its_own_tables(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log, {OTHER.name: None})
    gold, lake = writer(spark, log, serving=serving)
    lake.write(OTHER, spark.createDataFrame([(1,)], "customer_id long"))
    gold.unsynced.add(OTHER.name)
    log.clear()

    gold.write_batch("fact_order_item_history", spark.range(1))

    assert not any(line.startswith("copy") for line in log)
    assert gold.unsynced == {OTHER.name}


def test_a_batch_gives_up_after_the_last_retry(spark: SparkSession) -> None:
    log: list[str] = []
    planned = [(ITEMS.name, items(spark, (5, 2)))]
    gold, _ = writer(
        spark, log, planned, conflicts={ITEMS.name: 3}, serving=FakeServing(log), max_retries=2
    )

    with pytest.raises(ConflictError):
        gold.write_batch("fact_order_item_history", spark.range(1))

    assert log.count("compute fact_order_item_history") == 3


def test_any_other_failure_fails_the_batch_at_once(spark: SparkSession) -> None:
    log: list[str] = []
    gold, _ = writer(spark, log, [("missing", items(spark))], serving=FakeServing(log))

    with pytest.raises(KeyError):
        gold.write_batch("fact_order_item_history", spark.range(1))

    assert log == ["stage fact_order_item_history", "compute fact_order_item_history"]


@pytest.mark.parametrize(
    ("message", "conflict"),
    [
        (CONFLICT, True),
        ("org.apache.iceberg.exceptions.ValidationException: Found conflicting files", True),
        ("org.apache.iceberg.exceptions.ValidationException: Cannot find field", False),
        ("Connection refused", False),
    ],
)
def test_only_iceberg_commit_conflicts_are_retried(message: str, *, conflict: bool) -> None:
    assert is_commit_conflict(RuntimeError(message)) is conflict


def test_without_a_serving_copy_the_written_tables_wait_for_a_sync(spark: SparkSession) -> None:
    log: list[str] = []
    planned = [(SEEDS.name, seeds(spark, (1, "PENDING", True)))]
    gold, lake = writer(spark, log, planned)

    gold.write_batch("fact_order_item_history", spark.range(1))

    assert log[-1] == "lake write dim_order_status 1"
    assert gold.unsynced == {SEEDS.name}
    assert lake.snapshot(SEEDS.name) == 1


def test_each_batch_logs_one_line_with_its_steps_jobs_and_rows(
    spark: SparkSession, caplog: pytest.LogCaptureFixture
) -> None:
    log: list[str] = []
    planned = [(SEEDS.name, seeds(spark)), (ITEMS.name, items(spark, (5, 2)))]
    lake = FakeLake(tables={ITEMS.name: items(spark)}, log=log)
    jobs = FakeJobs({1})
    gold = GoldWriter(FakeBatches(planned, log), lake, FakeServing(log), jobs=jobs, clock=Ticks())

    with caplog.at_level(logging.INFO, logger=LOGGER):
        gold.write_batch("fact_order_item_history", spark.range(1), 9)

    (record,) = caplog.records
    line = json.loads(record.getMessage().removeprefix(BATCH_PREFIX))
    assert line["load"] == "fact_order_item_history"
    assert line["batch_id"] == 9
    assert line["written"] == {SEEDS.name: 0, ITEMS.name: 1}
    assert [step["step"] for step in line["steps"]] == [
        "catch-up",
        "compute dim_order_status",
        "compute current_order_items",
        "merge current_order_items",
        "snapshots",
        "copy",
    ]


def test_a_sync_copies_only_the_tables_whose_snapshot_the_copy_lacks(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log, {SEEDS.name: 1, ITEMS.name: 1, OTHER.name: None})
    gold, lake = writer(spark, log, serving=serving)
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))
    lake.write(ITEMS, items(spark, (5, 2), (6, 1)))
    lake.write(ITEMS, items(spark, (5, 2), (6, 3), (7, 1)))
    log.clear()

    copied = gold.sync([SEEDS.name, ITEMS.name])

    assert copied == [ITEMS.name]
    (change,) = serving.changes[-1]
    assert change.copy is Copy.UPSERT
    assert sorted((row.order_item_id, row.quantity) for row in change.rows.collect()) == [
        (6, 3),
        (7, 1),
    ]


def test_a_table_whose_synced_snapshot_is_gone_is_reloaded_whole(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log, {SEEDS.name: 7})
    gold, lake = writer(spark, log, serving=serving)
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))

    gold.sync([SEEDS.name])

    (change,) = serving.changes[-1]
    assert change.copy is Copy.RELOAD
    assert serving.synced_snapshots[SEEDS.name] == 1


def test_a_table_the_copy_holds_no_snapshot_of_is_copied_whole_as_upserts(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    serving = FakeServing(log, {SEEDS.name: None})
    gold, lake = writer(spark, log, serving=serving)
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))

    gold.sync([SEEDS.name])

    (change,) = serving.changes[-1]
    assert change.copy is Copy.UPSERT
    assert change.rows.count() == 1


def test_a_reload_copies_every_table_whole(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log)
    gold, _ = writer(spark, log, serving=serving)

    copied = gold.sync(reload=True)

    assert copied == list(gold.tables)
    assert {change.copy for change in serving.changes[-1]} == {Copy.RELOAD}


def test_the_seeds_write_only_the_rows_that_changed(spark: SparkSession) -> None:
    log: list[str] = []
    gold, lake = writer(spark, log, serving=FakeServing(log))
    lake.tables[SEEDS.name] = seeds(spark, (1, "PENDING", True))

    gold.write_seeds({SEEDS.name: seeds(spark, (1, "PENDING", True), (2, "DONE", False))})

    assert "lake write dim_order_status 1" in log
    assert sorted(tuple(row) for row in lake.read(SEEDS.name).collect()) == [(2, "DONE", False)]


def test_the_difference_of_two_snapshots_is_the_rows_added_or_changed(
    spark: SparkSession,
) -> None:
    before = seeds(spark, (1, "PENDING", True), (2, "DONE", False))
    after = seeds(spark, (1, "PENDING", True), (2, "DONE", True))

    change = difference(SEEDS, before, after)

    assert change.copy is Copy.UPSERT
    assert [tuple(row) for row in change.rows.collect()] == [(2, "DONE", True)]
