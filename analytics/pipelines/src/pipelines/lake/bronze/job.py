import os
import sys
from collections.abc import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from pipelines.lake.bronze.entities import entity_for_topic
from pipelines.lake.bronze.parse import parse_changes
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.streams import (
    TableCreator,
    TableSink,
    TableStream,
    append_to_iceberg,
    await_queries,
    start_streams,
    wants_available_now,
)
from pipelines.lake.shared.tables import ensure_table, table_definition
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

type LandingReader = Callable[[SparkSession, str], DataFrame]

BRONZE = "bronze"
QUARANTINE = "bronze_quarantine"


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    records = spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path)
    return records.select("*", F.col("_metadata.file_path").alias(MetadataColumn.LANDING_FILE))


def quarantined(rows: DataFrame) -> DataFrame:
    return rows.withColumn(MetadataColumn.QUARANTINED_AT, F.current_timestamp())


def entity_streams(
    spark: SparkSession, settings: BronzeSettings, topic: str, read: LandingReader
) -> tuple[TableStream, TableStream]:
    entity = entity_for_topic(topic)
    parsed = parse_changes(read(spark, settings.landing_path(topic)), entity)
    return (
        TableStream.for_table(
            settings, entity.name, parsed.bronze, table_definition(BRONZE, entity.definition)
        ),
        TableStream.for_table(
            settings,
            f"{entity.name}_quarantine",
            quarantined(parsed.quarantine),
            table_definition(QUARANTINE, entity.definition),
        ),
    )


def run(
    spark: SparkSession,
    settings: BronzeSettings,
    *,
    read: LandingReader = read_landing,
    create: TableCreator = ensure_table,
    sink: TableSink = append_to_iceberg,
    available_now: bool = False,
) -> list[StreamingQuery]:
    streams = [
        stream
        for topic in settings.topics
        for stream in entity_streams(spark, settings, topic, read)
    ]
    return start_streams(
        spark, streams, settings, create=create, sink=sink, available_now=available_now
    )


def main(argv: Sequence[str]) -> None:
    available_now = wants_available_now(argv, prog="bronze")
    settings = BronzeSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("bronze").getOrCreate()
    try:
        queries = run(spark, settings, available_now=available_now)
        await_queries(spark.streams, queries, available_now=available_now)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
