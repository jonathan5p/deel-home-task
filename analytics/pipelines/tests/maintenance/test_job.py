from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.checkpoints_setup import (
    FakeCheckpoints,
    offsets,
    snapshot_offset,
)
from analytics.pipelines.tests.maintenance.job_setup import (
    FakeSql,
    data_files,
    downstream,
    hours_from_now,
    leave_file,
    maintenance_settings,
    small_appends,
    snapshot_operations,
    stream_into,
    table_rows,
)
from pipelines.lake.checkpoints import HadoopCheckpointFiles
from pipelines.maintenance.job import (
    MaintenanceFailedError,
    TableMaintenance,
    run,
    spark_sql,
    sql_timestamp,
    tables,
)

ORDERS = "bronze.orders"
NO_STREAMS = FakeCheckpoints()
EXPIRE_EVERYTHING_OLD = {
    "MAINTENANCE_SNAPSHOT_RETENTION_HOURS": "1",
    "MAINTENANCE_RETAIN_LAST": "2",
}


def test_tables_are_listed_per_configured_namespace_and_missing_namespaces_are_skipped(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    small_appends(spark, f"{iceberg_catalog}.bronze.orders", 1)
    small_appends(spark, f"{iceberg_catalog}.bronze.customers", 1)
    small_appends(spark, f"{iceberg_catalog}.other.things", 1)

    found = tables(spark_sql(spark), maintenance_settings(iceberg_catalog))

    assert found == ["bronze.customers", "bronze.orders"]


def test_compaction_leaves_fewer_data_files_and_the_same_rows(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    small_appends(spark, table, 6)
    before = table_rows(spark, table)

    (result,) = run(spark_sql(spark), maintenance_settings(iceberg_catalog), NO_STREAMS)

    assert data_files(spark, table) == 1
    assert table_rows(spark, table) == before
    assert (result.rewritten_files, result.added_files) == (6, 1)
    assert snapshot_operations(spark, table)[-1] == "replace"


def test_snapshots_within_the_retention_are_kept(spark: SparkSession, iceberg_catalog: str) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    small_appends(spark, table, 6)

    (result,) = run(spark_sql(spark), maintenance_settings(iceberg_catalog), NO_STREAMS)

    assert result.expired_snapshots == 0
    assert len(snapshot_operations(spark, table)) == 7


def test_snapshots_older_than_the_retention_expire_except_the_last_ones(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    small_appends(spark, table, 6)
    settings = maintenance_settings(iceberg_catalog, **EXPIRE_EVERYTHING_OLD)

    (result,) = run(spark_sql(spark), settings, NO_STREAMS, now=hours_from_now(2))

    assert snapshot_operations(spark, table) == ["append", "replace"]
    assert result.expired_snapshots == 5
    assert len(table_rows(spark, table)) == 6


def test_only_unreferenced_files_older_than_the_orphan_age_are_removed(
    spark: SparkSession, iceberg_catalog: str, tmp_path: Path
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    small_appends(spark, table, 1)
    data = tmp_path / "warehouse" / "bronze" / "orders" / "data"
    old = leave_file(data, "old-orphan.parquet", age_hours=100)
    recent = leave_file(data, "recent-orphan.parquet", age_hours=1)

    (result,) = run(spark_sql(spark), maintenance_settings(iceberg_catalog), NO_STREAMS)

    assert result.removed_orphans == 1
    assert not old.exists()
    assert recent.exists()
    assert len(table_rows(spark, table)) == 1


def test_a_caught_up_stream_lets_old_snapshots_expire_and_keeps_reading(
    spark: SparkSession, iceberg_catalog: str, tmp_path: Path
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    stream = tmp_path / "stream"
    small_appends(spark, table, 6)
    stream_into(spark, table, stream)
    settings = maintenance_settings(
        iceberg_catalog, **EXPIRE_EVERYTHING_OLD, **downstream(ORDERS, stream / "checkpoint")
    )

    (result,) = run(spark_sql(spark), settings, HadoopCheckpointFiles(spark), now=hours_from_now(2))
    small_appends(spark, table, 2, first_id=6)

    assert not result.expiry_skipped
    assert result.expired_snapshots == 5
    assert [row.id for row in stream_into(spark, table, stream)] == list(range(8))


def test_a_lagging_stream_keeps_every_snapshot_and_catches_up(
    spark: SparkSession, iceberg_catalog: str, tmp_path: Path
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    stream = tmp_path / "stream"
    small_appends(spark, table, 3)
    stream_into(spark, table, stream)
    small_appends(spark, table, 3, first_id=3)
    settings = maintenance_settings(
        iceberg_catalog, **EXPIRE_EVERYTHING_OLD, **downstream(ORDERS, stream / "checkpoint")
    )
    reported: list[str] = []

    (result,) = run(
        spark_sql(spark),
        settings,
        HadoopCheckpointFiles(spark),
        now=hours_from_now(2),
        report=reported.append,
    )

    assert result.expiry_skipped
    assert len(snapshot_operations(spark, table)) == 7
    assert any("WARNING skipping snapshot expiry" in line for line in reported)
    assert [row.id for row in stream_into(spark, table, stream)] == list(range(6))


def test_a_stream_that_never_committed_does_not_hold_back_expiry(
    spark: SparkSession, iceberg_catalog: str, tmp_path: Path
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    small_appends(spark, table, 6)
    settings = maintenance_settings(
        iceberg_catalog, **EXPIRE_EVERYTHING_OLD, **downstream(ORDERS, tmp_path / "never-started")
    )
    reported: list[str] = []

    (result,) = run(
        spark_sql(spark),
        settings,
        HadoopCheckpointFiles(spark),
        now=hours_from_now(2),
        report=reported.append,
    )

    assert result.expired_snapshots == 5
    assert any("no committed batch" in line for line in reported)


def test_a_stream_whose_snapshot_is_already_gone_blocks_expiry(
    spark: SparkSession, iceberg_catalog: str, tmp_path: Path
) -> None:
    table = f"{iceberg_catalog}.{ORDERS}"
    stream = tmp_path / "stream"
    small_appends(spark, table, 3)
    stream_into(spark, table, stream)
    small_appends(spark, table, 2, first_id=3)
    spark.sql(
        f"CALL {iceberg_catalog}.system.expire_snapshots(table => '{ORDERS}',"
        f" older_than => {sql_timestamp(datetime.now(UTC) + timedelta(hours=1))},"
        " retain_last => 1)"
    )
    settings = maintenance_settings(
        iceberg_catalog, **EXPIRE_EVERYTHING_OLD, **downstream(ORDERS, stream / "checkpoint")
    )
    reported: list[str] = []

    (result,) = run(
        spark_sql(spark),
        settings,
        HadoopCheckpointFiles(spark),
        now=hours_from_now(2),
        report=reported.append,
    )

    assert result.expiry_skipped
    assert any("no longer exists" in line for line in reported)


def test_expiry_waits_for_every_stream_reading_the_table() -> None:
    files = {
        "s3a://lake/checkpoints/a/commits/0": "",
        "s3a://lake/checkpoints/a/offsets/0": offsets(snapshot_offset(11)),
        "s3a://lake/checkpoints/b/commits/0": "",
        "s3a://lake/checkpoints/b/offsets/0": offsets(snapshot_offset(12)),
    }
    sql = FakeSql({"bronze": ["orders"]}, expiring={11: False, 12: True})
    settings = maintenance_settings(
        "lake",
        MAINTENANCE_DOWNSTREAM_CHECKPOINTS=(
            "bronze.orders=s3a://lake/checkpoints/a|s3a://lake/checkpoints/b"
        ),
    )
    reported: list[str] = []

    (result,) = run(sql, settings, FakeCheckpoints(files), report=reported.append)

    assert result.expiry_skipped
    assert not any(".expire_snapshots(" in statement for statement in sql.statements)
    assert reported[0] == (
        "bronze.orders: WARNING skipping snapshot expiry:"
        " s3a://lake/checkpoints/b still needs snapshot 12"
    )
    assert result.removed_orphans == 1


def test_procedures_use_cutoffs_from_the_clock_and_settings() -> None:
    sql = FakeSql({"bronze": ["orders"]})
    settings = maintenance_settings("lake", MAINTENANCE_RETAIN_LAST="3")
    moment = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)

    run(sql, settings, NO_STREAMS, now=lambda: moment, report=lambda _: None)

    assert sql.statements[2:] == [
        "CALL lake.system.rewrite_data_files(table => 'bronze.orders', strategy => 'binpack')",
        "CALL lake.system.expire_snapshots(table => 'bronze.orders',"
        " older_than => TIMESTAMP '2026-11-03 12:00:00+00:00', retain_last => 3)",
        "CALL lake.system.remove_orphan_files(table => 'bronze.orders',"
        " older_than => TIMESTAMP '2026-11-07 12:00:00+00:00', prefix_listing => true)",
    ]


def test_a_failing_table_does_not_stop_the_others_and_fails_the_job() -> None:
    sql = FakeSql({"bronze": ["customers", "orders"]}, failing=frozenset({"bronze.customers"}))
    reported: list[str] = []

    with pytest.raises(MaintenanceFailedError) as error:
        run(sql, maintenance_settings("lake"), NO_STREAMS, report=reported.append)

    assert error.value.tables == ("bronze.customers",)
    assert reported == [
        "bronze.customers: maintenance failed: table metadata is unreadable",
        "bronze.orders: rewrote 6 data files into 1, expired 4 snapshots, removed 1 orphan files",
    ]


def test_every_table_is_reported() -> None:
    reported: list[str] = []

    maintained = run(
        FakeSql({"bronze": ["customers", "orders"]}),
        maintenance_settings("lake"),
        NO_STREAMS,
        report=reported.append,
    )

    assert [result.table for result in maintained] == ["bronze.customers", "bronze.orders"]
    assert reported == [result.summary() for result in maintained]


def test_a_summary_names_every_count() -> None:
    summary = TableMaintenance(ORDERS, 12, 2, 5, 0).summary()

    assert summary == (
        "bronze.orders: rewrote 12 data files into 2, expired 5 snapshots, removed 0 orphan files"
    )


def test_a_summary_says_when_expiry_was_skipped() -> None:
    summary = TableMaintenance(ORDERS, 12, 2, 0, 0, expiry_skipped=True).summary()

    assert "kept every snapshot for a lagging stream" in summary


def test_timestamps_are_written_in_utc() -> None:
    moment = datetime(2026, 11, 10, 14, 30, tzinfo=timezone(timedelta(hours=2)))

    assert sql_timestamp(moment) == "TIMESTAMP '2026-11-10 12:30:00+00:00'"
