import argparse
import os
import sys
from collections.abc import Callable, Sequence
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType

from pipelines.bronze.entities import entity_for_topic
from pipelines.bronze.parse import parse_changes
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

type LandingReader = Callable[[SparkSession, str], DataFrame]
type TableCreator = Callable[[SparkSession, str, StructType], None]
type TableSink = Callable[[DataStreamWriter, str], StreamingQuery]


class BronzeStream(NamedTuple):
    name: str
    table: str
    checkpoint: str
    rows: DataFrame


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    # The file source follows the landing file sink's _spark_metadata log, so files left behind
    # by failed landing micro-batches are never read.
    records = spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path)
    return records.select("*", F.col("_metadata.file_path").alias("landing_file"))


def column_definitions(schema: StructType) -> str:
    return ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields)


def create_table(spark: SparkSession, table: str, schema: StructType) -> None:
    namespace, _, _ = table.rpartition(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    # Streaming disables adaptive execution, so the default hash distribution would shuffle
    # every micro-batch into spark.sql.shuffle.partitions tasks; a local sort is enough here.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions(schema)}) USING iceberg"
        " PARTITIONED BY (days(published_at))"
        " TBLPROPERTIES ('write.distribution-mode' = 'none')"
    )


def append_to_iceberg(writer: DataStreamWriter, table: str) -> StreamingQuery:
    # Iceberg's sink records the query id and epoch in each snapshot and skips an epoch it has
    # already committed, so a micro-batch replayed after a restart is not appended twice.
    return writer.format("iceberg").outputMode("append").toTable(table)


def quarantined(rows: DataFrame) -> DataFrame:
    return rows.withColumn("quarantined_at", F.current_timestamp())


def _stream(settings: BronzeSettings, name: str, rows: DataFrame) -> BronzeStream:
    return BronzeStream(
        name=f"bronze {name}",
        table=settings.table(name),
        checkpoint=settings.checkpoint_path(name),
        rows=rows,
    )


def entity_streams(
    spark: SparkSession, settings: BronzeSettings, topic: str, read: LandingReader
) -> tuple[BronzeStream, BronzeStream]:
    entity = entity_for_topic(topic)
    parsed = parse_changes(read(spark, settings.landing_path(topic)), entity)
    return (
        _stream(settings, entity.name, parsed.bronze),
        _stream(settings, f"{entity.name}_quarantine", quarantined(parsed.quarantine)),
    )


def start_stream(
    stream: BronzeStream, settings: BronzeSettings, sink: TableSink, *, available_now: bool
) -> StreamingQuery:
    writer = stream.rows.writeStream.queryName(stream.name).option(
        "checkpointLocation", stream.checkpoint
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return sink(writer, stream.table)


def run(
    spark: SparkSession,
    settings: BronzeSettings,
    *,
    read: LandingReader = read_landing,
    create: TableCreator = create_table,
    sink: TableSink = append_to_iceberg,
    available_now: bool = False,
) -> list[StreamingQuery]:
    streams = [
        stream
        for topic in settings.topics
        for stream in entity_streams(spark, settings, topic, read)
    ]
    for stream in streams:
        create(spark, stream.table, stream.rows.schema)
    return [start_stream(stream, settings, sink, available_now=available_now) for stream in streams]


def wants_available_now(argv: Sequence[str]) -> bool:
    parser = argparse.ArgumentParser(prog="bronze")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process the landed records available at start, then stop",
    )
    return bool(parser.parse_args(argv).available_now)


def main(argv: Sequence[str]) -> None:
    available_now = wants_available_now(argv)
    settings = BronzeSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("bronze").getOrCreate()
    queries = run(spark, settings, available_now=available_now)
    if available_now:
        for query in queries:
            query.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main(sys.argv[1:])
