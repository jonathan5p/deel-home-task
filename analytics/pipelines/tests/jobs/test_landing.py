from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import lit

from pipelines.jobs.landing import TopicReader, landing_records, read_topic, run
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingSettings

TOPICS = ("cdc.orders", "cdc.customers")


def kafka_rows(topic: str, offsets: range, *, tombstone_at: int | None = None) -> list[Row]:
    published = datetime(2026, 11, 8, 14, 21)  # Spark returns naive timestamps in UTC
    return [
        Row(
            key=f'{{"id":{offset}}}'.encode(),
            value=None if offset == tombstone_at else f'{{"op":"c","id":{offset}}}'.encode(),
            topic=topic,
            partition=0,
            offset=offset,
            timestamp=published,
            timestampType=0,
        )
        for offset in offsets
    ]


def landing_settings(tmp_path: Path) -> LandingSettings:
    return LandingSettings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPICS": ",".join(TOPICS),
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
        }
    )


def file_reader(source: Path) -> TopicReader:
    def read(spark: SparkSession, kafka: KafkaSettings, topic: str) -> DataFrame:
        return (
            spark.readStream.schema(KAFKA_RECORD_SCHEMA)
            .option("recursiveFileLookup", "true")
            .parquet(str(source / topic))
        )

    return read


def stage(spark: SparkSession, source: Path, rows: list[Row], batch: str) -> None:
    topic = rows[0].topic
    spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).write.parquet(str(source / topic / batch))


def landed(spark: SparkSession, settings: LandingSettings, topic: str) -> list[Row]:
    records = spark.read.format("avro").load(settings.landing_path(topic))
    return sorted(records.collect(), key=lambda row: row.offset)


def run_until_caught_up(spark: SparkSession, settings: LandingSettings, source: Path) -> None:
    for query in run(spark, settings, read=file_reader(source), available_now=True):
        query.awaitTermination()


def test_landing_records_keep_exactly_the_kafka_record(spark: SparkSession) -> None:
    rows = kafka_rows("cdc.orders", range(3))
    records = spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).withColumn(
        "headers", lit(None).cast("string")
    )

    result = landing_records(records)

    assert result.schema == KAFKA_RECORD_SCHEMA
    assert result.collect() == rows


def test_read_topic_streams_the_kafka_record_schema(spark: SparkSession) -> None:
    kafka = KafkaSettings(bootstrap_servers="broker:9092", topics=("topic-a",))

    records = read_topic(spark, kafka, "topic-a")

    assert records.isStreaming
    assert landing_records(records).schema == KAFKA_RECORD_SCHEMA


def test_each_topic_lands_unchanged_in_its_own_folder(spark: SparkSession, tmp_path: Path) -> None:
    settings = landing_settings(tmp_path)
    source = tmp_path / "source"
    orders = kafka_rows("cdc.orders", range(4), tombstone_at=3)
    customers = kafka_rows("cdc.customers", range(2))
    stage(spark, source, orders, "batch-1")
    stage(spark, source, customers, "batch-1")

    run_until_caught_up(spark, settings, source)

    assert landed(spark, settings, "cdc.orders") == orders
    assert landed(spark, settings, "cdc.customers") == customers
    assert landed(spark, settings, "cdc.orders")[3].value is None
    landing_folder = Path(settings.landing_path("cdc.orders").removeprefix("file://"))
    assert any(path.suffix == ".avro" for path in landing_folder.iterdir())
    assert not any(path.is_dir() and "=" in path.name for path in landing_folder.iterdir())


def test_a_restart_resumes_from_the_checkpoint_without_gaps_or_duplicates(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = landing_settings(tmp_path)
    source = tmp_path / "source"
    first = kafka_rows("cdc.orders", range(0, 3))
    second = kafka_rows("cdc.orders", range(3, 6))
    stage(spark, source, first, "batch-1")
    stage(spark, source, kafka_rows("cdc.customers", range(1)), "batch-1")
    run_until_caught_up(spark, settings, source)

    stage(spark, source, second, "batch-2")
    run_until_caught_up(spark, settings, source)

    offsets = [row.offset for row in landed(spark, settings, "cdc.orders")]
    assert offsets == list(range(6))


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = landing_settings(tmp_path)
    source = tmp_path / "source"
    stage(spark, source, kafka_rows("cdc.orders", range(1)), "batch-1")
    stage(spark, source, kafka_rows("cdc.customers", range(1)), "batch-1")

    queries = run(spark, settings, read=file_reader(source))
    try:
        assert [query.name for query in queries] == [f"landing {topic}" for topic in TOPICS]
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()
