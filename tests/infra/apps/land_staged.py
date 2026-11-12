import os
import runpy
import sys

from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.landing.job import TopicReader, run
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingSettings


def staged_records(root: str) -> TopicReader:
    def read(spark: SparkSession, kafka: KafkaSettings, topic: str) -> DataFrame:
        return spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(f"{root}/{topic}")

    return read


def main(argv: list[str]) -> None:
    """Run the landing job on staged Avro files instead of Kafka until it has caught up, then run
    the app with its arguments as its own script in the same driver, so it needs no driver of
    its own.

    The landing folders then hold the file sink's commit log, as in production. The app's
    `getOrCreate` finds the landing's session, whose settings are the ones that `make
    spark-submit` gives the app.
    """
    staging_root, app, *app_args = argv
    settings = LandingSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("land-staged").getOrCreate()
    for query in run(spark, settings, read=staged_records(staging_root), available_now=True):
        query.awaitTermination()
    spark.streams.resetTerminated()
    sys.argv = [app, *app_args]
    runpy.run_path(app, run_name="__main__")


if __name__ == "__main__":
    main(sys.argv[1:])
