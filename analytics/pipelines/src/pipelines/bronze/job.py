import argparse
import contextlib
import os
import sys
from collections.abc import Callable, Sequence
from typing import NamedTuple, Self

from pyspark.errors import StreamingQueryException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery, StreamingQueryManager
from pyspark.sql.types import StructType

from pipelines.bronze.columns import MetadataColumn
from pipelines.bronze.entities import entity_for_topic
from pipelines.bronze.parse import parse_changes
from pipelines.bronze.tables import (
    TableDefinition,
    bronze_table,
    described_schema,
    ensure_table,
    quarantine_table,
)
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

type LandingReader = Callable[[SparkSession, str], DataFrame]
type TableCreator = Callable[[SparkSession, str, StructType, str], object]
type TableSink = Callable[[DataStreamWriter, str], StreamingQuery]


class BronzeStream(NamedTuple):
    name: str
    table: str
    checkpoint: str
    rows: DataFrame
    definition: TableDefinition

    @classmethod
    def for_table(
        cls, settings: BronzeSettings, name: str, rows: DataFrame, definition: TableDefinition
    ) -> Self:
        return cls(
            name=f"bronze {name}",
            table=settings.table(name),
            checkpoint=settings.checkpoint_path(name),
            rows=rows,
            definition=definition,
        )

    def table_schema(self) -> StructType:
        return described_schema(self.table, self.rows.schema, self.definition)


class StreamsFailedError(RuntimeError):
    def __init__(self, names: Sequence[str]) -> None:
        super().__init__(f"streaming queries failed: {', '.join(names)}")


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    records = spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path)
    return records.select("*", F.col("_metadata.file_path").alias(MetadataColumn.LANDING_FILE))


def append_to_iceberg(writer: DataStreamWriter, table: str) -> StreamingQuery:
    return writer.format("iceberg").outputMode("append").toTable(table)


def quarantined(rows: DataFrame) -> DataFrame:
    return rows.withColumn(MetadataColumn.QUARANTINED_AT, F.current_timestamp())


def entity_streams(
    spark: SparkSession, settings: BronzeSettings, topic: str, read: LandingReader
) -> tuple[BronzeStream, BronzeStream]:
    entity = entity_for_topic(topic)
    parsed = parse_changes(read(spark, settings.landing_path(topic)), entity)
    return (
        BronzeStream.for_table(settings, entity.name, parsed.bronze, bronze_table(entity)),
        BronzeStream.for_table(
            settings,
            f"{entity.name}_quarantine",
            quarantined(parsed.quarantine),
            quarantine_table(entity),
        ),
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
    create: TableCreator = ensure_table,
    sink: TableSink = append_to_iceberg,
    available_now: bool = False,
) -> list[StreamingQuery]:
    streams = [
        stream
        for topic in settings.topics
        for stream in entity_streams(spark, settings, topic, read)
    ]
    schemas = [stream.table_schema() for stream in streams]
    for stream, schema in zip(streams, schemas, strict=True):
        create(spark, stream.table, schema, stream.definition.comment)
    return [start_stream(stream, settings, sink, available_now=available_now) for stream in streams]


def await_queries(
    manager: StreamingQueryManager, queries: Sequence[StreamingQuery], *, available_now: bool
) -> None:
    """A continuous run stops every query as soon as one terminates, so one failed stream stops
    the whole job instead of leaving it half running; an available-now run waits for them all."""
    try:
        if available_now:
            for query in queries:
                with contextlib.suppress(StreamingQueryException):
                    query.awaitTermination()
        else:
            with contextlib.suppress(StreamingQueryException):
                manager.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
    failed = [query for query in queries if query.exception() is not None]
    if failed:
        raise StreamsFailedError([query.name for query in failed]) from failed[0].exception()


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
    try:
        queries = run(spark, settings, available_now=available_now)
        await_queries(spark.streams, queries, available_now=available_now)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
