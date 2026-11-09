from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, Row, SparkSession

from pipelines.quality.checks import CheckResult
from pipelines.quality.landing import (
    RECORD_SCHEMA,
    read_landing,
    reconcile,
)
from pipelines.settings import LandingSettings

PUBLISHED = datetime(2026, 11, 8, 14, 21)  # Spark returns naive timestamps in UTC
TOPIC = "topic-a"


def record(offset: int, *, value: bytes | None = b"v", topic: str = TOPIC) -> Row:
    return Row(
        key=f"k{offset}".encode(),
        value=value,
        topic=topic,
        partition=0,
        offset=offset,
        timestamp=PUBLISHED,
        timestampType=0,
    )


def frame(spark: SparkSession, rows: list[Row]) -> DataFrame:
    return spark.createDataFrame(rows, RECORD_SCHEMA)


def by_check(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {result.check: result for result in results}


def test_a_complete_landing_passes_and_reports_the_lag(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(6)])
    landed = frame(spark, [record(offset) for offset in range(4)])

    results = by_check(reconcile(landed, kafka))

    assert set(results) == {"landed", "unique", "contiguous", "complete_from_start", "identical"}
    assert all(result.passed for result in results.values())
    assert results["landed"].subject == "topic-a[0]"
    assert results["landed"].metrics == {
        "rows": 4,
        "min_offset": 0,
        "max_offset": 3,
        "tombstones": 0,
        "kafka_earliest": 0,
        "kafka_latest": 5,
        "lag": 2,
    }


def test_a_duplicated_offset_fails_uniqueness(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(3)])
    landed = frame(spark, [record(0), record(1), record(1), record(2)])

    results = by_check(reconcile(landed, kafka))

    assert not results["unique"].passed
    assert results["unique"].metrics == {"rows": 4, "distinct_offsets": 3}


def test_a_missing_offset_fails_contiguity(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(4)])
    landed = frame(spark, [record(0), record(1), record(3)])

    results = by_check(reconcile(landed, kafka))

    assert not results["contiguous"].passed
    assert results["contiguous"].metrics == {"distinct_offsets": 3, "expected_offsets": 4}


def test_a_changed_value_fails_the_byte_comparison(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(3)])
    landed = frame(spark, [record(0), record(1, value=b"changed"), record(2)])

    results = by_check(reconcile(landed, kafka))

    assert not results["identical"].passed
    assert results["identical"].metrics == {"compared": 3, "different": 1, "missing_in_kafka": 0}


def test_landing_that_starts_after_the_earliest_kafka_offset_fails(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(4)])
    landed = frame(spark, [record(2), record(3)])

    results = by_check(reconcile(landed, kafka))

    assert not results["complete_from_start"].passed
    assert results["complete_from_start"].metrics == {"min_offset": 2, "kafka_earliest": 0}


def test_tombstones_are_counted_and_compared(spark: SparkSession) -> None:
    kafka = frame(spark, [record(0), record(1, value=None)])
    landed = frame(spark, [record(0), record(1, value=None)])

    results = by_check(reconcile(landed, kafka))

    assert results["landed"].metrics["tombstones"] == 1
    assert results["identical"].passed


def test_a_kafka_partition_that_never_landed_fails(spark: SparkSession) -> None:
    kafka = frame(spark, [record(0, topic="topic-b")])
    landed = frame(spark, [])

    results = reconcile(landed, kafka)

    assert results == [
        CheckResult(
            check="landed",
            subject="topic-b[0]",
            passed=False,
            metrics={"rows": 0, "kafka_earliest": 0, "kafka_latest": 0},
        )
    ]


def test_a_window_checks_only_the_latest_landed_offsets(spark: SparkSession) -> None:
    kafka = frame(spark, [record(offset) for offset in range(6)])
    landed = frame(spark, [record(0), record(2), record(3), record(4), record(5)])

    results = by_check(reconcile(landed, kafka, last_offsets=3))

    assert results["contiguous"].passed
    assert results["landed"].metrics["min_offset"] == 3
    assert "complete_from_start" not in results


def test_a_missing_landing_folder_reads_as_no_records(spark: SparkSession, tmp_path: Path) -> None:
    settings = LandingSettings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPICS": TOPIC,
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
        }
    )

    landed = read_landing(spark, settings)

    assert landed.count() == 0
    assert landed.schema == RECORD_SCHEMA


def test_read_landing_reads_every_topic_folder(spark: SparkSession, tmp_path: Path) -> None:
    settings = LandingSettings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPICS": "topic-a,topic-b",
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
        }
    )
    for topic in ("topic-a", "topic-b"):
        rows = [record(0, topic=topic), record(1, topic=topic)]
        frame(spark, rows).write.format("avro").save(settings.landing_path(topic))

    landed = read_landing(spark, settings)

    assert sorted((row.topic, row.offset) for row in landed.collect()) == [
        ("topic-a", 0),
        ("topic-a", 1),
        ("topic-b", 0),
        ("topic-b", 1),
    ]
