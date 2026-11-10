from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from analytics.pipelines.tests.bronze.job_setup import BronzeSettingsFactory, land
from analytics.pipelines.tests.bronze.parse_samples import PUBLISHED, topic
from analytics.pipelines.tests.bronze.replay_setup import (
    FIRST_LSN,
    created,
    invalid,
    kafka_records,
    offsets_of,
    placed,
    refusal,
    replay_offsets,
    run_bronze,
)
from pipelines.bronze.entities import CUSTOMERS, ORDER_ITEMS
from pipelines.bronze.replay import (
    OffsetRange,
    ReplayRequest,
    ReplaySummary,
    TimeRange,
    change_identity,
    parse_request,
    replay,
)

PUBLISHED_UTC = PUBLISHED.replace(tzinfo=UTC)


def test_the_change_identity_is_the_entity_key_commit_sequence_and_op() -> None:
    assert change_identity(ORDER_ITEMS) == ("order_item_id", "_commit_sequence", "_change_op")


class TestRequest:
    def test_an_offset_range(self) -> None:
        request = parse_request(["--topic", "t", "--from-offset", "3", "--to-offset", "9"])

        assert request == ReplayRequest("t", OffsetRange(3, 9))

    def test_an_offset_range_in_one_partition(self) -> None:
        request = parse_request(
            ["--topic", "t", "--from-offset", "0", "--to-offset", "0", "--partition", "2"]
        )

        assert request.range == OffsetRange(0, 0, partition=2)

    def test_a_time_range_reads_times_without_an_offset_as_utc(self) -> None:
        request = parse_request(
            [
                "--topic",
                "t",
                "--from-time",
                "2026-11-08T14:00:00",
                "--to-time",
                "2026-11-08T16:00:00+01:00",
            ]
        )

        assert request.range == TimeRange(
            datetime(2026, 11, 8, 14, tzinfo=UTC), datetime(2026, 11, 8, 15, tzinfo=UTC)
        )

    def test_no_range_is_refused(self, capsys: pytest.CaptureFixture[str]) -> None:
        error = refusal(["--topic", "t"], capsys)

        assert "give a range" in error

    def test_an_offset_range_without_its_last_offset_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(["--topic", "t", "--from-offset", "1"], capsys)

        assert "give a range" in error

    def test_a_time_range_without_its_first_time_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(["--topic", "t", "--to-time", "2026-11-08T14:00:00"], capsys)

        assert "give a range" in error

    def test_an_offset_range_and_a_time_range_together_are_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(
            [
                "--topic",
                "t",
                "--from-offset",
                "1",
                "--to-offset",
                "2",
                "--from-time",
                "2026-11-08T14:00:00",
            ],
            capsys,
        )

        assert "not both" in error

    def test_a_first_offset_after_the_last_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(["--topic", "t", "--from-offset", "5", "--to-offset", "4"], capsys)

        assert "must not be after" in error

    def test_a_first_time_after_the_last_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(
            [
                "--topic",
                "t",
                "--from-time",
                "2026-11-09T00:00:00",
                "--to-time",
                "2026-11-08T00:00:00",
            ],
            capsys,
        )

        assert "must not be after" in error

    def test_a_partition_with_a_time_range_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(
            [
                "--topic",
                "t",
                "--from-time",
                "2026-11-08T00:00:00",
                "--to-time",
                "2026-11-09T00:00:00",
                "--partition",
                "0",
            ],
            capsys,
        )

        assert "only applies to an offset range" in error

    def test_a_time_that_is_not_iso_8601_is_refused(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error = refusal(["--topic", "t", "--from-time", "yesterday", "--to-time", "today"], capsys)

        assert "invalid iso_time value" in error

    def test_the_topic_is_required(self, capsys: pytest.CaptureFixture[str]) -> None:
        error = refusal(["--from-offset", "0", "--to-offset", "1"], capsys)

        assert "--topic" in error


class TestRange:
    def test_an_offset_range_keeps_both_ends(self, spark: SparkSession) -> None:
        records = kafka_records(spark, [created(CUSTOMERS, offset) for offset in range(5)])

        kept = records.where(OffsetRange(1, 3).contains()).collect()

        assert sorted(row.offset for row in kept) == [1, 2, 3]

    def test_an_offset_range_in_one_partition_leaves_the_others_out(
        self, spark: SparkSession
    ) -> None:
        rows = [created(CUSTOMERS, 0), placed(created(CUSTOMERS, 0), partition=1)]

        kept = kafka_records(spark, rows).where(OffsetRange(0, 0, partition=1).contains())

        assert [row.partition for row in kept.collect()] == [1]

    def test_a_time_range_keeps_both_ends(self, spark: SparkSession) -> None:
        rows = [
            placed(created(CUSTOMERS, offset), timestamp=PUBLISHED + timedelta(minutes=offset))
            for offset in range(4)
        ]
        window = TimeRange(
            PUBLISHED_UTC + timedelta(minutes=1), PUBLISHED_UTC + timedelta(minutes=2)
        )

        kept = kafka_records(spark, rows).where(window.contains()).collect()

        assert sorted(row.offset for row in kept) == [1, 2]


class TestReplay:
    def test_a_range_already_in_bronze_adds_nothing(
        self, spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
    ) -> None:
        settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
        land(spark, settings, [created(CUSTOMERS, 0), created(CUSTOMERS, 1)], "batch-1")
        run_bronze(spark, settings)

        summary = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 1)

        assert summary == ReplaySummary(topic(CUSTOMERS), 2, 0, 2, 0, 0)
        assert offsets_of(spark.table(settings.table("customers"))) == [0, 1]

    def test_missing_changes_are_added_once_each(
        self, spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
    ) -> None:
        settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
        land(spark, settings, [created(CUSTOMERS, 0)], "batch-1")
        run_bronze(spark, settings)
        repeated = created(CUSTOMERS, 3, lsn=FIRST_LSN + 1)
        land(spark, settings, [created(CUSTOMERS, 1), created(CUSTOMERS, 2), repeated], "batch-2")

        first = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 3)
        second = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 3)

        assert (first.appended, first.already_in_bronze) == (2, 1)
        assert (second.appended, second.already_in_bronze) == (0, 3)
        stored = spark.table(settings.table("customers"))
        assert stored.count() == 3
        assert {row._commit_sequence for row in stored.collect()} == {
            FIRST_LSN,
            FIRST_LSN + 1,
            FIRST_LSN + 2,
        }

    def test_failures_are_quarantined_once(
        self, spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
    ) -> None:
        settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
        land(spark, settings, [created(CUSTOMERS, 0), invalid(CUSTOMERS, 1)], "batch-1")

        first = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 1)
        second = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 1)

        assert (first.quarantined, first.already_quarantined) == (1, 0)
        assert (second.quarantined, second.already_quarantined) == (0, 1)
        quarantine = spark.table(settings.table("customers_quarantine"))
        assert offsets_of(quarantine) == [1]

    def test_a_replay_creates_missing_tables(
        self, spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
    ) -> None:
        settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
        land(spark, settings, [created(CUSTOMERS, 0)], "batch-1")

        summary = replay_offsets(spark, settings, topic(CUSTOMERS), 0, 0)

        assert (summary.appended, summary.quarantined) == (1, 0)
        assert spark.catalog.tableExists(settings.table("customers"))
        assert spark.catalog.tableExists(settings.table("customers_quarantine"))

    def test_only_records_in_the_range_are_replayed(
        self, spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
    ) -> None:
        settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
        rows = [
            placed(created(CUSTOMERS, offset), timestamp=PUBLISHED + timedelta(hours=offset))
            for offset in range(3)
        ]
        land(spark, settings, rows, "batch-1")
        window = TimeRange(PUBLISHED_UTC + timedelta(hours=1), PUBLISHED_UTC + timedelta(hours=5))

        summary = replay(spark, settings, ReplayRequest(topic(CUSTOMERS), window))

        assert (summary.in_range, summary.appended) == (2, 2)
        assert offsets_of(spark.table(settings.table("customers"))) == [1, 2]

    def test_the_summary_reads_as_one_line(self) -> None:
        summary = ReplaySummary("t", 5, 2, 1, 1, 0)

        assert str(summary) == (
            "replay t: 5 records in range, 2 changes appended, 1 already in bronze,"
            " 1 quarantined, 0 already quarantined"
        )
