from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, Row, SparkSession

from pipelines.jobs.landing_quality import run_checks
from pipelines.quality.landing import RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingQualitySettings, LandingSettings

ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
    "KAFKA_TOPICS": "topic-a",
    "LAKE_ROOT": "s3a://bucket",
    "ICEBERG_CATALOG": "catalog",
}


def records(spark: SparkSession, offsets: range) -> DataFrame:
    rows = [
        Row(
            key=b"k",
            value=b"v",
            topic="topic-a",
            partition=0,
            offset=offset,
            timestamp=datetime(2026, 11, 8, 14, 21),  # Spark returns naive timestamps in UTC
            timestampType=0,
        )
        for offset in offsets
    ]
    return spark.createDataFrame(rows, RECORD_SCHEMA)


def test_run_checks_reconciles_the_injected_landing_and_kafka_records(spark: SparkSession) -> None:
    settings = LandingQualitySettings.from_env(ENV | {"LANDING_CHECK_LAST_OFFSETS": "2"})
    seen: list[str] = []

    def landed(session: SparkSession, landing: LandingSettings) -> DataFrame:
        seen.append(f"landing {landing.kafka.topics}")
        return records(session, range(4))

    def kafka(session: SparkSession, kafka_settings: KafkaSettings) -> DataFrame:
        seen.append(f"kafka {kafka_settings.bootstrap_servers}")
        return records(session, range(5))

    results = run_checks(spark, settings, read_landed=landed, read_records=kafka)

    assert seen == ["landing ('topic-a',)", "kafka broker:9092"]
    assert all(result.passed for result in results)
    assert "complete_from_start" not in {result.check for result in results}
