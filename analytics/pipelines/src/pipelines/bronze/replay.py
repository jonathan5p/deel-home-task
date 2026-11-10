import argparse
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NamedTuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.bronze.entities import Entity, entity_for_topic
from pipelines.bronze.job import BRONZE, QUARANTINE, quarantined
from pipelines.bronze.parse import parse_changes
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.streams import TableCreator
from pipelines.lake.tables import (
    TableDefinition,
    described_schema,
    ensure_table,
    table_definition,
)
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

type BatchReader = Callable[[SparkSession, str], DataFrame]

QUARANTINE_IDENTITY = (
    MetadataColumn.RECORD_SOURCE,
    MetadataColumn.SOURCE_PARTITION,
    MetadataColumn.SOURCE_OFFSET,
)


@dataclass(frozen=True, slots=True)
class OffsetRange:
    first: int
    last: int
    partition: int | None = None

    def contains(self) -> Column:
        within = F.col("offset").between(self.first, self.last)
        if self.partition is None:
            return within
        return within & (F.col("partition") == self.partition)


@dataclass(frozen=True, slots=True)
class TimeRange:
    start: datetime
    end: datetime

    def contains(self) -> Column:
        return F.col("timestamp").between(F.lit(self.start), F.lit(self.end))


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    topic: str
    range: OffsetRange | TimeRange


class ReplaySummary(NamedTuple):
    topic: str
    in_range: int
    appended: int
    already_in_bronze: int
    quarantined: int
    already_quarantined: int

    def __str__(self) -> str:
        return (
            f"replay {self.topic}: {self.in_range} records in range, {self.appended} changes"
            f" appended, {self.already_in_bronze} already in bronze, {self.quarantined}"
            f" quarantined, {self.already_quarantined} already quarantined"
        )


def iso_time(text: str) -> datetime:
    moment = datetime.fromisoformat(text)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def parse_request(argv: Sequence[str]) -> ReplayRequest:
    parser = argparse.ArgumentParser(
        prog="bronze-replay",
        description="Append the changes of a landing range that bronze does not hold yet.",
    )
    parser.add_argument("--topic", required=True, help="landing topic to replay")
    parser.add_argument("--from-offset", type=int, help="first Kafka offset, inclusive")
    parser.add_argument("--to-offset", type=int, help="last Kafka offset, inclusive")
    parser.add_argument("--partition", type=int, help="only this Kafka partition (offsets only)")
    parser.add_argument(
        "--from-time", type=iso_time, help="first publish time, ISO 8601, inclusive"
    )
    parser.add_argument("--to-time", type=iso_time, help="last publish time, ISO 8601, inclusive")
    args = parser.parse_args(argv)

    offsets = (args.from_offset, args.to_offset)
    times = (args.from_time, args.to_time)
    if any(value is not None for value in offsets) and any(value is not None for value in times):
        parser.error("give either an offset range or a time range, not both")
    if args.from_offset is not None and args.to_offset is not None:
        if args.from_offset > args.to_offset:
            parser.error("--from-offset must not be after --to-offset")
        return ReplayRequest(
            args.topic, OffsetRange(args.from_offset, args.to_offset, args.partition)
        )
    if args.partition is not None:
        parser.error("--partition only applies to an offset range")
    if args.from_time is not None and args.to_time is not None:
        if args.from_time > args.to_time:
            parser.error("--from-time must not be after --to-time")
        return ReplayRequest(args.topic, TimeRange(args.from_time, args.to_time))
    parser.error("give a range: --from-offset and --to-offset, or --from-time and --to-time")


def read_landed(spark: SparkSession, path: str) -> DataFrame:
    records = spark.read.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path)
    return records.select("*", F.col("_metadata.file_path").alias(MetadataColumn.LANDING_FILE))


def change_identity(entity: Entity) -> tuple[str, ...]:
    key = next(column.lake_name for column in entity.columns if column.name == entity.key)
    return (key, MetadataColumn.COMMIT_SEQUENCE, MetadataColumn.CHANGE_OP)


def missing(rows: DataFrame, stored: DataFrame, identity: Sequence[str]) -> DataFrame:
    """The rows whose identity `stored` lacks, one per identity."""
    columns = list(identity)
    return rows.dropDuplicates(columns).join(stored.select(*columns), columns, "left_anti")


def append_missing(
    spark: SparkSession,
    table: str,
    rows: DataFrame,
    definition: TableDefinition,
    identity: Sequence[str],
    create: TableCreator,
) -> tuple[int, int]:
    """Append the rows `table` lacks; returns how many were appended and how many it had."""
    create(spark, table, described_schema(table, rows.schema, definition), definition)
    candidates = rows.dropDuplicates(list(identity)).count()
    new = missing(rows, spark.table(table), identity).localCheckpoint()
    appended = new.count()
    if appended:
        new.writeTo(table).append()
    return appended, candidates - appended


def replay(
    spark: SparkSession,
    settings: BronzeSettings,
    request: ReplayRequest,
    *,
    read: BatchReader = read_landed,
    create: TableCreator = ensure_table,
) -> ReplaySummary:
    entity = entity_for_topic(request.topic)
    records = read(spark, settings.landing_path(request.topic))
    in_range = records.where(request.range.contains()).localCheckpoint()
    parsed = parse_changes(in_range, entity)
    appended, present = append_missing(
        spark,
        settings.table(entity.name),
        parsed.bronze,
        table_definition(BRONZE, entity.definition),
        change_identity(entity),
        create,
    )
    quarantine, already = append_missing(
        spark,
        settings.table(f"{entity.name}_quarantine"),
        quarantined(parsed.quarantine),
        table_definition(QUARANTINE, entity.definition),
        QUARANTINE_IDENTITY,
        create,
    )
    return ReplaySummary(
        topic=request.topic,
        in_range=in_range.count(),
        appended=appended,
        already_in_bronze=present,
        quarantined=quarantine,
        already_quarantined=already,
    )


def main(argv: Sequence[str]) -> None:
    request = parse_request(argv)
    settings = BronzeSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("bronze-replay").getOrCreate()
    try:
        print(replay(spark, settings, request), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
