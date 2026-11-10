from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from pyspark.sql import DataFrame, Row, SparkSession

from analytics.pipelines.tests.silver.versions_samples import (
    BronzeChange,
    bronze_frame,
    bronze_schema,
)
from pipelines.bronze.entities import Entity
from pipelines.bronze.tables import bronze_table
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.streams import await_queries
from pipelines.lake.tables import described_schema, ensure_table
from pipelines.settings import SilverSettings
from pipelines.silver.job import BronzeReader, run


class SilverSettingsFactory(Protocol):
    def __call__(self, *entities: Entity, catalog: str = "catalog") -> SilverSettings: ...


def topic(entity: Entity) -> str:
    return f"test.operations.{entity.name}"


def local(path: str) -> Path:
    return Path(urlparse(path).path)


def write_bronze(
    spark: SparkSession, settings: SilverSettings, entity: Entity, changes: list[BronzeChange]
) -> None:
    """Append changes to the entity's bronze table, created the way the bronze job creates it."""
    table = settings.bronze_table(entity.name)
    definition = bronze_table(entity)
    ensure_table(
        spark, table, described_schema(table, bronze_schema(entity), definition), definition
    )
    bronze_frame(spark, entity, changes).writeTo(table).append()


def run_until_caught_up(spark: SparkSession, settings: SilverSettings) -> None:
    await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)


def silver_rows(spark: SparkSession, settings: SilverSettings, entity: Entity) -> list[Row]:
    return (
        spark.table(settings.table(entity.name)).orderBy(MetadataColumn.COMMIT_SEQUENCE).collect()
    )


def commit_sequences(rows: list[Row]) -> list[int]:
    return [row[MetadataColumn.COMMIT_SEQUENCE] for row in rows]


def no_bronze_rows(entity: Entity) -> BronzeReader:
    """A bronze reader that finds nothing, for checks that only need the version schema."""

    def read(spark: SparkSession, table: str) -> DataFrame:
        return bronze_frame(spark, entity, [])

    return read
