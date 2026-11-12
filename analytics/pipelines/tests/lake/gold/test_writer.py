from collections.abc import Sequence

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.writer_setup import (
    BY_PRODUCT,
    CONFLICT,
    SEEDS,
    TOP,
    ConflictError,
    FakeBatches,
    FakeLake,
    FakeServing,
    RecordingLock,
    report,
    seeds,
)
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
) -> tuple[GoldWriter, FakeLake, FakeServing | None]:
    lake = FakeLake(
        tables={
            "dim_order_status": seeds(spark),
            "pending_items_by_product": report(spark),
            "top_delivery_dates_by_open_orders": spark.createDataFrame(
                [], "delivery_date date, open_orders long"
            ),
        },
        log=log,
        conflicts=conflicts or {},
    )
    batches = FakeBatches(list(planned), log)
    gold = GoldWriter(batches, lake, serving, lock=RecordingLock(log), max_retries=max_retries)
    return gold, lake, serving


def test_a_batch_is_written_to_iceberg_in_order_then_copied_in_one_go(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    planned = [
        ("dim_order_status", seeds(spark, (1, "PENDING", True))),
        ("pending_items_by_product", report(spark, (5, 1, 2))),
    ]
    gold, _, _ = writer(spark, log, planned, serving=FakeServing(log))

    gold.write_batch("fact_order_history", spark.range(1))

    assert log == [
        "lock",
        "compute fact_order_history",
        "lake write dim_order_status 1",
        "lake write pending_items_by_product 1",
        "copy [dim_order_status upsert 1, pending_items_by_product recount 1]"
        " {'dim_order_status': 1, 'pending_items_by_product': 1}",
        "unlock",
    ]


def test_writes_that_change_nothing_are_neither_committed_nor_copied(spark: SparkSession) -> None:
    log: list[str] = []
    gold, _, _ = writer(spark, log, [("dim_order_status", seeds(spark))], serving=FakeServing(log))

    gold.write_batch("fact_order_history", spark.range(1))

    assert log == ["lock", "compute fact_order_history", "unlock"]


def test_a_commit_conflict_replays_the_batch_after_copying_what_the_failed_try_wrote(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    planned = [
        ("dim_order_status", seeds(spark, (1, "PENDING", True))),
        ("pending_items_by_product", report(spark, (5, 1, 2))),
    ]
    serving = FakeServing(log, {"dim_order_status": None, "pending_items_by_product": None})
    gold, _, _ = writer(
        spark, log, planned, conflicts={"pending_items_by_product": 1}, serving=serving
    )

    gold.write_batch("fact_order_history", spark.range(1))

    assert log == [
        "lock",
        "compute fact_order_history",
        "lake write dim_order_status 1",
        "lake write pending_items_by_product 1",
        "copy [dim_order_status upsert 1] {'dim_order_status': 1}",
        "compute fact_order_history",
        "lake write dim_order_status 1",
        "lake write pending_items_by_product 1",
        "copy [dim_order_status upsert 1, pending_items_by_product recount 1]"
        " {'dim_order_status': 2, 'pending_items_by_product': 1}",
        "unlock",
    ]


def test_a_batch_gives_up_after_the_last_retry(spark: SparkSession) -> None:
    log: list[str] = []
    planned = [("pending_items_by_product", report(spark, (5, 1, 2)))]
    gold, _, _ = writer(
        spark,
        log,
        planned,
        conflicts={"pending_items_by_product": 3},
        serving=FakeServing(log),
        max_retries=2,
    )

    with pytest.raises(ConflictError):
        gold.write_batch("fact_order_history", spark.range(1))

    assert log.count("compute fact_order_history") == 3
    assert log[-1] == "unlock"


def test_any_other_failure_fails_the_batch_at_once(spark: SparkSession) -> None:
    log: list[str] = []
    gold, _, _ = writer(spark, log, [("missing", report(spark))], serving=FakeServing(log))

    with pytest.raises(KeyError):
        gold.write_batch("fact_order_history", spark.range(1))

    assert log == ["lock", "compute fact_order_history", "unlock"]


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
    planned = [("dim_order_status", seeds(spark, (1, "PENDING", True)))]
    gold, lake, _ = writer(spark, log, planned)

    gold.write_batch("fact_order_history", spark.range(1))

    assert log[-2:] == ["lake write dim_order_status 1", "unlock"]
    assert gold.unsynced == {"dim_order_status"}
    assert lake.snapshot("dim_order_status") == 1


def test_a_sync_copies_only_the_tables_whose_snapshot_the_copy_lacks(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(
        log,
        {
            "dim_order_status": 1,
            "pending_items_by_product": 1,
            "top_delivery_dates_by_open_orders": None,
        },
    )
    gold, lake, _ = writer(spark, log, serving=serving)
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))
    lake.write(BY_PRODUCT, report(spark, (5, 1, 2), (6, 1, 1)))
    lake.write(BY_PRODUCT, report(spark, (5, 2, 4)))
    log.clear()

    copied = gold.sync()

    assert copied == ["pending_items_by_product"]
    (change,) = serving.changes[-1]
    assert change.copy is Copy.RECOUNT
    assert sorted(tuple(row) for row in change.rows.collect()) == [
        (5, 2, 4, False),
        (6, 1, 1, True),
    ]


def test_a_table_whose_synced_snapshot_is_gone_is_reloaded_whole(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log, {"dim_order_status": 7})
    gold, lake, _ = writer(spark, log, serving=serving)
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))

    gold.sync(["dim_order_status"])

    (change,) = serving.changes[-1]
    assert change.copy is Copy.RELOAD
    assert serving.synced_snapshots["dim_order_status"] == 1


def test_a_reload_copies_every_table_whole(spark: SparkSession) -> None:
    log: list[str] = []
    serving = FakeServing(log)
    gold, _, _ = writer(spark, log, serving=serving)

    copied = gold.sync(reload=True)

    assert copied == list(gold.tables)
    assert {change.copy for change in serving.changes[-1]} == {Copy.RELOAD}


def test_the_seeds_write_only_the_rows_that_changed(spark: SparkSession) -> None:
    log: list[str] = []
    gold, lake, _ = writer(spark, log, serving=FakeServing(log))
    lake.tables["dim_order_status"] = seeds(spark, (1, "PENDING", True))

    gold.write_seeds({"dim_order_status": seeds(spark, (1, "PENDING", True), (2, "DONE", False))})

    assert "lake write dim_order_status 1" in log
    assert sorted(tuple(row) for row in lake.read("dim_order_status").collect()) == [
        (2, "DONE", False)
    ]


def test_a_replaced_table_is_copied_whole_from_any_snapshot(spark: SparkSession) -> None:
    rows = spark.createDataFrame([], "delivery_date date, open_orders long")

    assert difference(TOP, rows, rows).copy is Copy.REPLACE


def test_a_merged_table_copies_the_rows_added_or_changed(spark: SparkSession) -> None:
    before = seeds(spark, (1, "PENDING", True), (2, "DONE", False))
    after = seeds(spark, (1, "PENDING", True), (2, "DONE", True))

    change = difference(SEEDS, before, after)

    assert change.copy is Copy.UPSERT
    assert [tuple(row) for row in change.rows.collect()] == [(2, "DONE", True)]


def test_a_recounted_table_removes_the_keys_gone_nulls_matching(spark: SparkSession) -> None:
    before = report(spark, (None, 1, 1), (5, 1, 1))
    after = report(spark, (None, 1, 1))

    change = difference(BY_PRODUCT, before, after)

    assert [tuple(row) for row in change.rows.collect()] == [(5, 1, 1, True)]
