import argparse
import os
import sys
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.job import SilverReader, gold_writer, prepare, session_conf
from pipelines.lake.gold.lake import IcebergGold
from pipelines.lake.gold.postgres import connection_factory
from pipelines.lake.gold.retention import apply_retention
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.sink import BatchLoader, GoldLoad, staged_rows
from pipelines.lake.gold.writer import GoldWriter
from pipelines.settings import GoldRetentionSettings, GoldSettings

type Retain = Callable[[], list[str]]
type Prepare = Callable[[], None]


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
    writer: BatchLoader,
    recreate: Prepare,
    copy: Prepare,
    retain: Retain,
    dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
    facts: Iterable[ChangeFact] = FACTS,
    read: SilverReader = read_silver_table,
) -> list[str]:
    """Recreate the gold tables, write every silver change up to `to_time` with the gold job's
    writer, one batch per load and the dimensions first, then copy every table into
    PostgreSQL; returns the partitions retention dropped afterwards."""
    recreate()
    loads: list[GoldLoad] = [*dimensions, *facts]
    for load in loads:
        silver = until(read(spark, settings.source_table(load.entity.name)), to_time)
        writer.write_batch(load.name, staged_rows(silver, load))
    copy()
    return retain()


def recreated(spark: SparkSession, writer: GoldWriter, lake: IcebergGold) -> Prepare:
    def recreate() -> None:
        lake.drop()
        prepare(spark, writer, lake)

    return recreate


def main(argv: Sequence[str]) -> None:
    cutoff = to_time(argv)
    settings = GoldSettings.from_env(os.environ)
    retention = GoldRetentionSettings.from_env(os.environ)
    connect = connection_factory(retention.analytics_db)

    def retain() -> list[str]:
        return apply_retention(connect, retention, datetime.now(UTC).date())

    builder = SparkSession.builder.appName("gold-rebuild")
    for key, value in session_conf(settings, {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        writer, lake = gold_writer(spark, settings, serve=False)
        serving, _ = gold_writer(spark, settings)

        def copy() -> None:
            serving.sync(reload=True)

        dropped = run(
            spark,
            settings,
            to_time=cutoff,
            writer=writer,
            recreate=recreated(spark, writer, lake),
            copy=copy,
            retain=retain,
        )
    finally:
        spark.stop()
    scope = f"up to {cutoff.isoformat()}" if cutoff else "with every change"
    print(f"Gold rebuilt from silver {scope}; retention dropped {len(dropped)} partition(s)")
    for name in dropped:
        print(f"  {name}")


if __name__ == "__main__":
    main(sys.argv[1:])
