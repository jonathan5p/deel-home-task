from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from pipelines.landing.quality_job import run_checks
from pipelines.schemas import KAFKA_RECORD_SCHEMA
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
    return spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA)


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
    assert results
    assert all(result.passed for result in results)
    assert "starts_at_kafka_min_offset" not in {result.check for result in results}


# PySpark warns that a plain Python UDF's type hints do not name a pandas UDF type.
@pytest.mark.filterwarnings("ignore:Cannot infer the eval type:UserWarning")
def test_run_checks_reads_each_kafka_record_once(spark: SparkSession) -> None:
    record_count = 5
    settings = LandingQualitySettings.from_env(ENV)
    reads = spark.sparkContext.accumulator(0)

    def count_read(offset: int) -> int:
        reads.add(1)
        return offset

    read_offset = F.udf(count_read, LongType())

    def kafka(session: SparkSession, kafka_settings: KafkaSettings) -> DataFrame:
        return records(session, range(record_count)).withColumn("offset", read_offset("offset"))

    results = run_checks(
        spark,
        settings,
        read_landed=lambda session, _: records(session, range(record_count)),
        read_records=kafka,
    )

    assert all(result.passed for result in results)
    assert reads.value == record_count
