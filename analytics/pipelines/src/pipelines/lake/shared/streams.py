import argparse
import contextlib
from collections.abc import Callable, Sequence
from typing import ClassVar, NamedTuple, Protocol, Self

from pyspark.errors import StreamingQueryException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery, StreamingQueryManager
from pyspark.sql.types import StructType

from pipelines.lake.shared.tables import TableDefinition, described_schema

type TableCreator = Callable[[SparkSession, str, StructType, TableDefinition], object]
type TableSink = Callable[[DataStreamWriter, str], StreamingQuery]


class LayerSettings(Protocol):
    LAYER: ClassVar[str]

    @property
    def trigger_seconds(self) -> int: ...

    def table(self, name: str) -> str: ...

    def checkpoint_path(self, name: str) -> str: ...


class TableStream(NamedTuple):
    name: str
    table: str
    checkpoint: str
    rows: DataFrame
    definition: TableDefinition

    @classmethod
    def for_table(
        cls, settings: LayerSettings, name: str, rows: DataFrame, definition: TableDefinition
    ) -> Self:
        return cls(
            name=f"{settings.LAYER} {name}",
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


def append_to_iceberg(writer: DataStreamWriter, table: str) -> StreamingQuery:
    return writer.format("iceberg").outputMode("append").toTable(table)


def start_stream(
    stream: TableStream, settings: LayerSettings, sink: TableSink, *, available_now: bool
) -> StreamingQuery:
    writer = stream.rows.writeStream.queryName(stream.name).option(
        "checkpointLocation", stream.checkpoint
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return sink(writer, stream.table)


def start_streams(
    spark: SparkSession,
    streams: Sequence[TableStream],
    settings: LayerSettings,
    *,
    create: TableCreator,
    sink: TableSink,
    available_now: bool,
) -> list[StreamingQuery]:
    schemas = [stream.table_schema() for stream in streams]
    for stream, schema in zip(streams, schemas, strict=True):
        create(spark, stream.table, schema, stream.definition)
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


def wants_available_now(argv: Sequence[str], *, prog: str) -> bool:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process the input available at start, then stop",
    )
    return bool(parser.parse_args(argv).available_now)
