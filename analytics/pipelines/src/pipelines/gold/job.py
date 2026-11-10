import os
import sys
from collections.abc import Callable, Iterable, Sequence
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from common.settings import AnalyticsDbSettings
from pipelines.gold.dimensions import DIMENSIONS
from pipelines.gold.postgres import connection_factory
from pipelines.gold.scd2 import Scd2Dimension
from pipelines.gold.sink import BatchWriter, Scd2Sink, jdbc_stage, staged_rows
from pipelines.lake.streams import await_queries, wants_available_now
from pipelines.settings import GoldSettings

type SilverReader = Callable[[SparkSession, str], DataFrame]
type SinkFactory = Callable[[Scd2Dimension], BatchWriter]


class GoldStream(NamedTuple):
    name: str
    checkpoint: str
    rows: DataFrame
    sink: BatchWriter


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.readStream.table(table)


def scd2_sinks(settings: AnalyticsDbSettings) -> SinkFactory:
    stage = jdbc_stage(settings)
    connect = connection_factory(settings)

    def sink(dimension: Scd2Dimension) -> BatchWriter:
        return Scd2Sink(dimension, stage, connect)

    return sink


def dimension_stream(
    spark: SparkSession,
    settings: GoldSettings,
    dimension: Scd2Dimension,
    read: SilverReader,
    sink: SinkFactory,
) -> GoldStream:
    silver = read(spark, settings.source_table(dimension.entity.name))
    return GoldStream(
        name=f"gold {dimension.name}",
        checkpoint=settings.checkpoint_path(dimension.name),
        rows=staged_rows(silver, dimension),
        sink=sink(dimension),
    )


def start_stream(
    stream: GoldStream, settings: GoldSettings, *, available_now: bool
) -> StreamingQuery:
    writer = (
        stream.rows.writeStream.queryName(stream.name)
        .option("checkpointLocation", stream.checkpoint)
        .foreachBatch(stream.sink)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return writer.start()


def run(
    spark: SparkSession,
    settings: GoldSettings,
    *,
    dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
    read: SilverReader = read_silver,
    sink: SinkFactory | None = None,
    available_now: bool = False,
) -> list[StreamingQuery]:
    """Start one stream per SCD2 dimension, from its silver table to its gold table."""
    sinks = sink or scd2_sinks(settings.analytics_db)
    streams = [
        dimension_stream(spark, settings, dimension, read, sinks) for dimension in dimensions
    ]
    return [start_stream(stream, settings, available_now=available_now) for stream in streams]


def main(argv: Sequence[str]) -> None:
    available_now = wants_available_now(argv, prog="gold")
    settings = GoldSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("gold").getOrCreate()
    try:
        queries = run(spark, settings, available_now=available_now)
        await_queries(spark.streams, queries, available_now=available_now)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
