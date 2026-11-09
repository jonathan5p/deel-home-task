import argparse
import contextlib
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import NamedTuple, Self

from pyspark.errors import StreamingQueryException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery, StreamingQueryManager
from pyspark.sql.types import StructType

from pipelines.bronze.columns import LANDING_FILE, PUBLISHED_AT, QUARANTINED_AT
from pipelines.bronze.descriptions import (
    TableDescription,
    UndescribedColumnError,
    bronze_table_description,
    quarantine_table_description,
)
from pipelines.bronze.entities import entity_for_topic
from pipelines.bronze.parse import parse_changes
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

type LandingReader = Callable[[SparkSession, str], DataFrame]
type TableCreator = Callable[[SparkSession, str, StructType, TableDescription], None]
type TableSink = Callable[[DataStreamWriter, str], StreamingQuery]


class BronzeStream(NamedTuple):
    name: str
    table: str
    checkpoint: str
    rows: DataFrame
    description: TableDescription

    @classmethod
    def for_table(
        cls, settings: BronzeSettings, name: str, rows: DataFrame, description: TableDescription
    ) -> Self:
        return cls(
            name=f"bronze {name}",
            table=settings.table(name),
            checkpoint=settings.checkpoint_path(name),
            rows=rows,
            description=description,
        )


class StreamsFailedError(RuntimeError):
    def __init__(self, names: Sequence[str]) -> None:
        super().__init__(f"streaming queries failed: {', '.join(names)}")


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    records = spark.readStream.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path)
    return records.select("*", F.col("_metadata.file_path").alias(LANDING_FILE))


def sql_string(text: str) -> str:
    """A Spark SQL string literal that reads back as `text`."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def column_definitions(schema: StructType, comments: Mapping[str, str]) -> str:
    return ", ".join(
        f"`{field.name}` {field.dataType.simpleString()} COMMENT {sql_string(comments[field.name])}"
        for field in schema.fields
    )


def table_ddl(table: str, schema: StructType, description: TableDescription) -> str:
    """The CREATE TABLE statement for `table`, with a comment on the table and every column."""
    undescribed = description.undescribed(schema.fieldNames())
    if undescribed:
        raise UndescribedColumnError(table, undescribed)
    # Streaming disables adaptive execution, so the default hash distribution would shuffle
    # every micro-batch into spark.sql.shuffle.partitions tasks; a local sort is enough here.
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions(schema, description.columns)})"
        " USING iceberg"
        f" PARTITIONED BY (days({PUBLISHED_AT}))"
        f" COMMENT {sql_string(description.comment)}"
        " TBLPROPERTIES ('write.distribution-mode' = 'none')"
    )


def create_table(
    spark: SparkSession, table: str, schema: StructType, description: TableDescription
) -> None:
    ddl = table_ddl(table, schema, description)
    namespace, _, _ = table.rpartition(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(ddl)


def append_to_iceberg(writer: DataStreamWriter, table: str) -> StreamingQuery:
    return writer.format("iceberg").outputMode("append").toTable(table)


def quarantined(rows: DataFrame) -> DataFrame:
    return rows.withColumn(QUARANTINED_AT, F.current_timestamp())


def entity_streams(
    spark: SparkSession, settings: BronzeSettings, topic: str, read: LandingReader
) -> tuple[BronzeStream, BronzeStream]:
    entity = entity_for_topic(topic)
    parsed = parse_changes(read(spark, settings.landing_path(topic)), entity)
    return (
        BronzeStream.for_table(
            settings, entity.name, parsed.bronze, bronze_table_description(entity)
        ),
        BronzeStream.for_table(
            settings,
            f"{entity.name}_quarantine",
            quarantined(parsed.quarantine),
            quarantine_table_description(entity),
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
        create(spark, stream.table, stream.rows.schema, stream.description)
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
