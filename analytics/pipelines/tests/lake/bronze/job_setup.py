import shutil
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from pyspark.sql import DataFrame, Row, SparkSession

from analytics.pipelines.tests.lake.bronze.parse_samples import (
    Json,
    change,
    landing,
    landing_records,
)
from analytics.pipelines.tests.lake.shared.streams_setup import FakeCatalog
from pipelines.lake.bronze.entities import Entity
from pipelines.lake.bronze.job import run
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.streams import await_queries
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings


def record(entity: Entity, offset: int, value: Json | str | None) -> Row:
    """A Kafka record as the landing job writes it, without the reader's `_landing_file`."""
    landed = landing(entity, value, offset=offset)
    return Row(**{name: landed[name] for name in KAFKA_RECORD_SCHEMA.fieldNames()})


def valid(entity: Entity, offset: int) -> Row:
    return record(entity, offset, change(entity, "c"))


class BronzeSettingsFactory(Protocol):
    def __call__(self, *topics: str, catalog: str = "catalog") -> BronzeSettings: ...


def local(path: str) -> Path:
    return Path(urlparse(path).path)


def land(spark: SparkSession, settings: BronzeSettings, rows: list[Row], batch: str) -> None:
    """Land records like the landing job: through a file sink, which keeps a commit log."""
    landing_topic = rows[0].topic
    staging = local(settings.lake.root) / "staging" / landing_topic
    spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).write.parquet(str(staging / batch))
    query = (
        spark.readStream.schema(KAFKA_RECORD_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(str(staging))
        .writeStream.format("avro")
        .option("path", settings.landing_path(landing_topic))
        .option("checkpointLocation", f"{settings.lake.checkpoint_root}/landing/{landing_topic}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def drop_stray_file(spark: SparkSession, settings: BronzeSettings, rows: list[Row]) -> Path:
    """Leave an Avro file in a landing folder without recording it in the commit log."""
    elsewhere = local(settings.lake.root) / "stray"
    spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).coalesce(1).write.format("avro").save(
        str(elsewhere)
    )
    (avro,) = elsewhere.glob("*.avro")
    stray = local(settings.landing_path(rows[0].topic)) / "part-stray.avro"
    shutil.move(avro, stray)
    return stray


def source_coordinates(row: Row) -> tuple[str, int]:
    return row[MetadataColumn.RECORD_SOURCE], row[MetadataColumn.SOURCE_OFFSET]


def run_until_caught_up(
    spark: SparkSession, settings: BronzeSettings, catalog: FakeCatalog
) -> None:
    queries = run(spark, settings, create=catalog.create, sink=catalog.sink(), available_now=True)
    await_queries(spark.streams, queries, available_now=True)


def offsets(rows: list[Row]) -> list[int]:
    return [row[MetadataColumn.SOURCE_OFFSET] for row in rows]


def no_landed_records(spark: SparkSession, path: str) -> DataFrame:
    """A landing reader that finds nothing, for checks that only need the parsed schemas."""
    return landing_records(spark)
