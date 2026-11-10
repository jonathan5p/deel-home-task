import argparse
import os
import sys
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.gold.dimensions import DIMENSIONS, FACTS
from pipelines.gold.facts import ChangeFact
from pipelines.gold.job import SilverReader, SinkFactory, merge_sinks
from pipelines.gold.postgres import connection_factory
from pipelines.gold.retention import apply_retention
from pipelines.gold.scd2 import Scd2Dimension
from pipelines.gold.sink import GoldLoad, staged_rows
from pipelines.settings import GoldRetentionSettings, GoldSettings

type Retain = Callable[[], list[str]]


def iso_time(value: str) -> datetime:
    """An ISO 8601 time; one without an offset is UTC, like the gold timestamps."""
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{value!r} is not an ISO 8601 time") from error
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def to_time(argv: Sequence[str]) -> datetime | None:
    parser = argparse.ArgumentParser(
        prog="gold-rebuild", description="Rebuild gold from the silver tables."
    )
    parser.add_argument(
        "--to-time",
        type=iso_time,
        help="last source commit time (_changed_at) to load, ISO 8601, inclusive; default: all",
    )
    return parser.parse_args(argv).to_time


def read_silver_table(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.table(table)


def until(silver: DataFrame, cutoff: datetime | None) -> DataFrame:
    """The silver changes committed at or before `cutoff`, or all of them without one."""
    if cutoff is None:
        return silver
    return silver.where(F.col("_changed_at") <= F.lit(cutoff))


def run(
    spark: SparkSession,
    settings: GoldSettings,
    *,
    to_time: datetime | None,
    retain: Retain,
    dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
    facts: Iterable[ChangeFact] = FACTS,
    read: SilverReader = read_silver_table,
    sink: SinkFactory | None = None,
) -> list[str]:
    """Load every silver change up to `to_time` into gold, one batch per load through the gold
    job's own sinks, then apply retention; returns the partitions retention dropped.

    The dimensions go first, so every fact resolves its dimension keys against complete
    dimensions; the merges re-resolve each other, so the order changes no result."""
    facts = tuple(facts)
    sinks = sink or merge_sinks(settings.analytics_db, facts)
    loads: list[GoldLoad] = [*dimensions, *facts]
    for load in loads:
        silver = until(read(spark, settings.source_table(load.entity.name)), to_time)
        sinks(load)(staged_rows(silver, load), 0)
    return retain()


def main(argv: Sequence[str]) -> None:
    cutoff = to_time(argv)
    settings = GoldSettings.from_env(os.environ)
    retention = GoldRetentionSettings.from_env(os.environ)
    connect = connection_factory(retention.analytics_db)

    def retain() -> list[str]:
        return apply_retention(connect, retention, datetime.now(UTC).date())

    spark = SparkSession.builder.appName("gold-rebuild").getOrCreate()
    try:
        dropped = run(spark, settings, to_time=cutoff, retain=retain)
    finally:
        spark.stop()
    scope = f"up to {cutoff.isoformat()}" if cutoff else "with every change"
    print(f"Gold rebuilt from silver {scope}; retention dropped {len(dropped)} partition(s)")
    for name in dropped:
        print(f"  {name}")


if __name__ == "__main__":
    main(sys.argv[1:])
