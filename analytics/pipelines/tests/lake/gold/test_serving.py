from dataclasses import replace
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.postgres_setup import FakeConnection, StatementFailedError
from analytics.pipelines.tests.lake.gold.serving_setup import (
    BY_PRODUCT,
    HISTORY,
    SEED_SCHEMA,
    SEEDS,
    TOP,
    history_rows,
    report_rows,
)
from pipelines.lake.gold.postgres import WRITER_LOCK
from pipelines.lake.gold.serving import (
    SYNC_STATE_QUERY,
    Change,
    Copy,
    PostgresServing,
    copy_statements,
    delete_removed_statement,
    staged,
    sync_statement,
    upsert_statement,
)

WINDOW = datetime(2023, 11, 1)


def test_an_upsert_inserts_the_staged_rows_and_updates_the_other_columns_of_a_stored_key() -> None:
    assert upsert_statement(SEEDS) == (
        "INSERT INTO gold_internal.dim_order_status (status_key, status_code, is_open)"
        " SELECT status_key, status_code, is_open FROM gold_internal.stage_dim_order_status"
        " ON CONFLICT (status_key) DO UPDATE"
        " SET status_code = EXCLUDED.status_code, is_open = EXCLUDED.is_open"
    )


def test_a_history_upsert_conflicts_on_the_key_with_the_change_time() -> None:
    assert " ON CONFLICT (order_id, _commit_sequence, _changed_at) DO UPDATE SET " in (
        upsert_statement(HISTORY)
    )


def test_a_table_whose_columns_are_all_keys_is_upserted_without_updates() -> None:
    statement = upsert_statement(replace(BY_PRODUCT, keys=BY_PRODUCT.columns))

    assert statement.endswith(" DO NOTHING")


def test_removed_report_rows_are_deleted_by_their_key_nulls_matching() -> None:
    assert delete_removed_statement(BY_PRODUCT) == (
        "DELETE FROM gold_internal.pending_items_by_product AS target"
        " USING gold_internal.stage_pending_items_by_product AS staged"
        " WHERE staged._is_removed AND target.product_id IS NOT DISTINCT FROM staged.product_id"
    )


@pytest.mark.parametrize(
    ("copy", "first"),
    [
        (Copy.UPSERT, "INSERT INTO gold_internal.pending_items_by_product"),
        (Copy.RECOUNT, "DELETE FROM gold_internal.pending_items_by_product AS target"),
        (Copy.REPLACE, "DELETE FROM gold_internal.pending_items_by_product"),
        (Copy.RELOAD, "TRUNCATE gold_internal.pending_items_by_product"),
    ],
)
def test_each_copy_starts_with_its_statement(spark: SparkSession, copy: Copy, first: str) -> None:
    statements = copy_statements(Change(BY_PRODUCT, spark.range(0).toDF("id"), copy))

    assert statements[0].startswith(first)


def test_a_recount_upserts_only_the_rows_not_removed(spark: SparkSession) -> None:
    _, upsert = copy_statements(Change(BY_PRODUCT, spark.range(0).toDF("id"), Copy.RECOUNT))

    assert upsert == upsert_statement(BY_PRODUCT, "NOT _is_removed")
    assert " WHERE NOT _is_removed ON CONFLICT (product_id) " in upsert


def test_a_replace_and_a_reload_insert_every_staged_row(spark: SparkSession) -> None:
    for copy in (Copy.REPLACE, Copy.RELOAD):
        _, insert = copy_statements(Change(TOP, spark.range(0).toDF("id"), copy))

        assert insert == (
            "INSERT INTO gold_internal.top_delivery_dates_by_open_orders"
            " (delivery_date, open_orders) SELECT delivery_date, open_orders"
            " FROM gold_internal.stage_top_delivery_dates_by_open_orders"
        )


def test_the_sync_state_records_each_snapshot_or_none() -> None:
    assert sync_statement({"dim_date": 41, "dim_customer": None}) == (
        "INSERT INTO gold_internal.sync_state (table_name, snapshot_id, synced_at)"
        " VALUES ('dim_date', 41, now()), ('dim_customer', NULL, now())"
        " ON CONFLICT (table_name) DO UPDATE"
        " SET snapshot_id = EXCLUDED.snapshot_id, synced_at = EXCLUDED.synced_at"
    )


def test_history_older_than_the_retention_window_is_not_staged(spark: SparkSession) -> None:
    rows = history_rows(spark, [(1, 10, datetime(2023, 10, 31, 23)), (2, 20, WINDOW)])

    kept = staged(Change(HISTORY, rows, Copy.UPSERT), WINDOW).collect()

    assert [row.order_id for row in kept] == [2]


def test_staged_rows_take_the_columns_of_the_staging_table(spark: SparkSession) -> None:
    rows = report_rows(spark, [(5, 2, 7)])

    (row,) = staged(Change(BY_PRODUCT, rows, Copy.RELOAD), WINDOW).collect()

    assert row.asDict() == {
        "product_id": 5,
        "pending_items": 2,
        "pending_quantity": 7,
        "_is_removed": False,
    }


def test_the_synced_snapshots_are_read_from_the_sync_state() -> None:
    connection = FakeConnection(results={SYNC_STATE_QUERY: [("dim_date", 41), ("top", None)]})
    serving = PostgresServing(lambda rows, table: None, lambda: connection, lambda: WINDOW)

    assert serving.synced() == {"dim_date": 41, "top": None}


def test_the_changes_are_staged_then_copied_in_one_transaction_with_their_snapshots(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    connection = FakeConnection(log=log)
    rows = report_rows(spark, [(5, 2, 7)])

    def stage(staged_rows: DataFrame, table: str) -> None:
        log.append(f"stage {table} {staged_rows.count()}")

    serving = PostgresServing(stage, lambda: connection, lambda: WINDOW)
    serving.apply([Change(BY_PRODUCT, rows, Copy.RECOUNT)], {"pending_items_by_product": 9})

    assert log == [
        "stage gold_internal.stage_pending_items_by_product 1",
        f"execute {WRITER_LOCK}",
        f"execute {delete_removed_statement(BY_PRODUCT)}",
        f"execute {upsert_statement(BY_PRODUCT, 'NOT _is_removed')}",
        f"execute {sync_statement({'pending_items_by_product': 9})}",
        "commit",
        "close",
    ]


def test_nothing_is_copied_without_a_snapshot(spark: SparkSession) -> None:
    log: list[str] = []
    serving = PostgresServing(
        lambda rows, table: log.append(table), lambda: FakeConnection(log=log), lambda: WINDOW
    )

    serving.apply([], {})

    assert log == []


def test_a_failed_copy_rolls_back(spark: SparkSession) -> None:
    connection = FakeConnection(fail_on=upsert_statement(SEEDS))
    serving = PostgresServing(lambda rows, table: None, lambda: connection, lambda: WINDOW)
    rows = spark.createDataFrame([(1, "PENDING", True)], SEED_SCHEMA)

    with pytest.raises(StatementFailedError):
        serving.apply([Change(SEEDS, rows, Copy.UPSERT)], {"dim_order_status": 3})

    assert connection.log[-2:] == ["rollback", "close"]
