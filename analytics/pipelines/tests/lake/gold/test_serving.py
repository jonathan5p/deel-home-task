from dataclasses import replace
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.postgres_setup import FakeConnection, StatementFailedError
from analytics.pipelines.tests.lake.gold.serving_setup import (
    HISTORY,
    ITEMS,
    SEED_SCHEMA,
    SEEDS,
    history_rows,
    item_rows,
)
from pipelines.lake.gold.postgres import COPY_LOCK
from pipelines.lake.gold.serving import (
    SYNC_STATE_QUERY,
    Change,
    Copy,
    PostgresServing,
    copy_statements,
    insert_statement,
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
    statement = upsert_statement(replace(SEEDS, keys=SEEDS.columns))

    assert statement.endswith(" DO NOTHING")


def test_an_upsert_copies_the_staged_rows_with_one_statement(spark: SparkSession) -> None:
    statements = copy_statements(Change(ITEMS, spark.range(0).toDF("id")))

    assert statements == [upsert_statement(ITEMS)]


def test_a_reload_empties_the_table_then_inserts_every_staged_row(spark: SparkSession) -> None:
    statements = copy_statements(Change(ITEMS, spark.range(0).toDF("id"), Copy.RELOAD))

    assert statements == ["TRUNCATE gold_internal.current_order_items", insert_statement(ITEMS)]
    assert statements[1] == (
        "INSERT INTO gold_internal.current_order_items"
        " (order_item_id, order_id, product_id, quantity, _commit_sequence, _changed_at,"
        " _is_deleted) SELECT order_item_id, order_id, product_id, quantity, _commit_sequence,"
        " _changed_at, _is_deleted FROM gold_internal.stage_current_order_items"
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
    rows = item_rows(spark, [(5, 7)]).selectExpr("'extra' AS extra", "*")

    (row,) = staged(Change(ITEMS, rows, Copy.RELOAD), WINDOW).collect()

    assert list(row.asDict()) == list(ITEMS.columns)


def test_the_synced_snapshots_are_read_from_the_sync_state() -> None:
    connection = FakeConnection(results={SYNC_STATE_QUERY: [("dim_date", 41), ("top", None)]})
    serving = PostgresServing(lambda rows, table: None, lambda: connection, lambda: WINDOW)

    assert serving.synced() == {"dim_date": 41, "top": None}


def test_the_changes_are_staged_then_copied_in_one_transaction_with_their_snapshots(
    spark: SparkSession,
) -> None:
    log: list[str] = []
    connection = FakeConnection(log=log)
    rows = item_rows(spark, [(5, 7)])

    def stage(staged_rows: DataFrame, table: str) -> None:
        log.append(f"stage {table} {staged_rows.count()}")

    serving = PostgresServing(stage, lambda: connection, lambda: WINDOW)
    serving.apply([Change(ITEMS, rows)], {"current_order_items": 9})

    assert log == [
        "stage gold_internal.stage_current_order_items 1",
        f"execute {COPY_LOCK}",
        f"execute {upsert_statement(ITEMS)}",
        f"execute {sync_statement({'current_order_items': 9})}",
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
