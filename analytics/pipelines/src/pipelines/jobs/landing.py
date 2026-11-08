import os
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from pipelines.settings import KafkaSettings, LandingSettings

KAFKA_RECORD_COLUMNS = (
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "timestampType",
)

type TopicReader = Callable[[SparkSession, KafkaSettings, str], DataFrame]


def read_topic(spark: SparkSession, kafka: KafkaSettings, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )


def landing_records(records: DataFrame) -> DataFrame:
    return records.select(*KAFKA_RECORD_COLUMNS)


def start_landing(
    records: DataFrame, topic: str, settings: LandingSettings, *, available_now: bool = False
) -> StreamingQuery:
    writer = (
        landing_records(records)
        .writeStream.format("avro")
        .queryName(f"landing {topic}")
        .option("path", settings.landing_path(topic))
        .option("checkpointLocation", settings.checkpoint_path(topic))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return writer.start()


def run(
    spark: SparkSession,
    settings: LandingSettings,
    *,
    read: TopicReader = read_topic,
    available_now: bool = False,
) -> list[StreamingQuery]:
    return [
        start_landing(
            read(spark, settings.kafka, topic), topic, settings, available_now=available_now
        )
        for topic in settings.kafka.topics
    ]


def main() -> None:
    settings = LandingSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("landing").getOrCreate()
    run(spark, settings)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
