import os
import sys

from pyspark.sql import DataFrame, SparkSession

from pipelines.landing.job import TopicReader, run
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingSettings


def staged_records(root: str) -> TopicReader:
    def read(spark: SparkSession, kafka: KafkaSettings, topic: str) -> DataFrame:
        return spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(f"{root}/{topic}")

    return read


def main(argv: list[str]) -> None:
    """Run the landing job on staged Avro files instead of Kafka until it has caught up.

    The landing folders then hold the file sink's commit log, as in production.
    """
    (staging_root,) = argv
    settings = LandingSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("land-staged").getOrCreate()
    for query in run(spark, settings, read=staged_records(staging_root), available_now=True):
        query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
