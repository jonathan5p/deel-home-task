import os
import sys
from collections.abc import Callable, Iterable, Sequence
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.postgres import connection_factory
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.sink import BatchWriter, GoldLoad, MergeSink, jdbc_stage, staged_rows
from pipelines.lake.shared.streams import await_queries, wants_available_now
from pipelines.settings import GoldSettings

type SilverReader = Callable[[SparkSession, str], DataFrame]
type SinkFactory = Callable[[GoldLoad], BatchWriter]


class GoldStream(NamedTuple):
    name: str
    checkpoint: str
    rows: DataFrame
    sink: BatchWriter


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.readStream.table(table)


def merge_sinks(settings: AnalyticsDbSettings, facts: Iterable[ChangeFact] = FACTS) -> SinkFactory:
    """Sinks that merge each load and then re-resolve the `facts` that look up what it wrote."""
    stage = jdbc_stage(settings)
    connect = connection_factory(settings)
    dependents = tuple(facts)

    def sink(load: GoldLoad) -> BatchWriter:
        return MergeSink(load, stage, connect, dependents)

    return sink


def gold_stream(
    spark: SparkSession,
    settings: GoldSettings,
    load: GoldLoad,
    read: SilverReader,
    sink: SinkFactory,
) -> GoldStream:
    silver = read(spark, settings.source_table(load.entity.name))
    return GoldStream(
        name=f"gold {load.name}",
        checkpoint=settings.checkpoint_path(load.name),
        rows=staged_rows(silver, load),
        sink=sink(load),
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
    facts: Iterable[ChangeFact] = FACTS,
    read: SilverReader = read_silver,
    sink: SinkFactory | None = None,
    available_now: bool = False,
) -> list[StreamingQuery]:
    """Start one stream per SCD2 dimension and one per history fact, each from its silver
    table to its gold tables."""
    facts = tuple(facts)
    sinks = sink or merge_sinks(settings.analytics_db, facts)
    loads: list[GoldLoad] = [*dimensions, *facts]
    streams = [gold_stream(spark, settings, load, read, sinks) for load in loads]
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
