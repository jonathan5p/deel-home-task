import os
import sys
from collections.abc import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from pipelines.bronze.entities import entity_for_topic
from pipelines.lake.streams import (
    TableCreator,
    TableSink,
    TableStream,
    append_to_iceberg,
    await_queries,
    start_streams,
    wants_available_now,
)
from pipelines.lake.tables import ensure_table
from pipelines.settings import SilverSettings
from pipelines.silver.tables import silver_table
from pipelines.silver.versions import versions

type BronzeReader = Callable[[SparkSession, str], DataFrame]


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.readStream.table(table)


def entity_stream(
    spark: SparkSession, settings: SilverSettings, topic: str, read: BronzeReader
) -> TableStream:
    entity = entity_for_topic(topic)
    rows = versions(read(spark, settings.bronze_table(entity.name)), entity)
    return TableStream.for_table(settings, entity.name, rows, silver_table(entity))


def run(
    spark: SparkSession,
    settings: SilverSettings,
    *,
    read: BronzeReader = read_bronze,
    create: TableCreator = ensure_table,
    sink: TableSink = append_to_iceberg,
    available_now: bool = False,
) -> list[StreamingQuery]:
    streams = [entity_stream(spark, settings, topic, read) for topic in settings.topics]
    return start_streams(
        spark, streams, settings, create=create, sink=sink, available_now=available_now
    )


def main(argv: Sequence[str]) -> None:
    available_now = wants_available_now(argv, prog="silver")
    settings = SilverSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("silver").getOrCreate()
    try:
        queries = run(spark, settings, available_now=available_now)
        await_queries(spark.streams, queries, available_now=available_now)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
