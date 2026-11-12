import os
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from pipelines.lake.gold.batch import GoldBatches
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.lake import IcebergGold
from pipelines.lake.gold.model.seeds import calendar_days, order_statuses
from pipelines.lake.gold.postgres import connection_factory
from pipelines.lake.gold.retention import retained_from
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.serving import PostgresServing
from pipelines.lake.gold.sink import BatchWriter, GoldLoad, GoldSink, jdbc_stage, staged_rows
from pipelines.lake.gold.stored import StoredTable
from pipelines.lake.gold.writer import GoldWriter
from pipelines.lake.shared.streams import await_queries, wants_available_now
from pipelines.settings import GoldSettings

SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
SESSION_CONF: Mapping[str, str] = {SHUFFLE_PARTITIONS: "4"}

type SilverReader = Callable[[SparkSession, str], DataFrame]
type SinkFactory = Callable[[GoldLoad], BatchWriter]
type Today = Callable[[], date]


class GoldStream(NamedTuple):
    name: str
    checkpoint: str
    rows: DataFrame
    sink: BatchWriter


def session_conf(settings: GoldSettings, conf: Mapping[str, str] = SESSION_CONF) -> dict[str, str]:
    """The Spark settings of a gold session. Each stream runs its micro-batches in a session of
    its own, with its own catalog, so the catalog must not cache tables: a cached table would
    hide what the other streams committed, and a merge would read stale rows."""
    return {**conf, f"spark.sql.catalog.{settings.lake.catalog}.cache-enabled": "false"}


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.readStream.table(table)


def utc_today() -> date:
    return datetime.now(UTC).date()


def iceberg_gold(
    spark: SparkSession, settings: GoldSettings, tables: Mapping[str, StoredTable] = STORED
) -> IcebergGold:
    relations = {name: settings.stored_table(name) for name in tables}
    return IcebergGold(spark, tables, relations)


def postgres_serving(settings: GoldSettings, today: Today = utc_today) -> PostgresServing:
    def window() -> datetime:
        return retained_from(settings.retention_months, today())

    db = settings.analytics_db
    return PostgresServing(jdbc_stage(db), connection_factory(db), window)


def gold_writer(
    spark: SparkSession,
    settings: GoldSettings,
    *,
    dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
    facts: Iterable[ChangeFact] = FACTS,
    serve: bool = True,
) -> tuple[GoldWriter, IcebergGold]:
    """The gold writer on the lake's gold tables, copying into PostgreSQL when `serve`."""
    lake = iceberg_gold(spark, settings)
    serving = postgres_serving(settings) if serve else None
    return GoldWriter(GoldBatches(STORED, dimensions, facts), lake, serving), lake


def prepare(spark: SparkSession, writer: GoldWriter, lake: IcebergGold) -> None:
    """Create the gold tables that are missing, merge the seeded rows, then copy into
    PostgreSQL every table it does not hold the latest snapshot of."""
    lake.ensure()
    writer.write_seeds(
        {"dim_order_status": order_statuses(spark), "dim_date": calendar_days(spark)}
    )
    writer.sync()


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


def writer_sinks(writer: GoldWriter) -> SinkFactory:
    def sink(load: GoldLoad) -> BatchWriter:
        return GoldSink(load, writer)

    return sink


def run(
    spark: SparkSession,
    settings: GoldSettings,
    sink: SinkFactory,
    *,
    dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
    facts: Iterable[ChangeFact] = FACTS,
    read: SilverReader = read_silver,
    available_now: bool = False,
) -> list[StreamingQuery]:
    """Start one stream per SCD2 dimension and one per history fact, each from its silver
    table to its gold tables."""
    loads: list[GoldLoad] = [*dimensions, *facts]
    streams = [gold_stream(spark, settings, load, read, sink) for load in loads]
    return [start_stream(stream, settings, available_now=available_now) for stream in streams]


def main(argv: Sequence[str]) -> None:
    available_now = wants_available_now(argv, prog="gold")
    settings = GoldSettings.from_env(os.environ)
    builder = SparkSession.builder.appName("gold")
    for key, value in session_conf(settings).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        writer, lake = gold_writer(spark, settings)
        prepare(spark, writer, lake)
        queries = run(spark, settings, writer_sinks(writer), available_now=available_now)
        await_queries(spark.streams, queries, available_now=available_now)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
