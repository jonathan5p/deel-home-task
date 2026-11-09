import shutil
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType

from analytics.pipelines.tests.bronze.parse_samples import Json, change, landing, landing_records
from pipelines.bronze.columns import MetadataColumn
from pipelines.bronze.entities import Entity
from pipelines.bronze.job import TableSink, await_queries, run
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


class FakeCatalog:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.schemas: dict[str, StructType] = {}
        self.comments: dict[str, str] = {}

    def create(self, spark: SparkSession, table: str, schema: StructType, comment: str) -> None:
        self.schemas[table] = schema
        self.comments[table] = comment

    def sink(self) -> TableSink:
        def append(writer: DataStreamWriter, table: str) -> StreamingQuery:
            return writer.format("parquet").start(str(self.root / table))

        return append

    def rows(self, spark: SparkSession, table: str) -> list[Row]:
        stored = spark.read.schema(self.schemas[table]).parquet(str(self.root / table))
        return sorted(
            stored.collect(),
            key=lambda row: (row[MetadataColumn.RECORD_SOURCE], row[MetadataColumn.SOURCE_OFFSET]),
        )


def run_until_caught_up(
    spark: SparkSession, settings: BronzeSettings, catalog: FakeCatalog
) -> None:
    queries = run(spark, settings, create=catalog.create, sink=catalog.sink(), available_now=True)
    await_queries(spark.streams, queries, available_now=True)


def offsets(rows: list[Row]) -> list[int]:
    return [row[MetadataColumn.SOURCE_OFFSET] for row in rows]


def numbers(spark: SparkSession, folder: Path, count: int) -> DataFrame:
    """A file stream over `count` one-row files that reads one file per micro-batch."""
    for number in range(count):
        batch = spark.createDataFrame([(number,)], "number long")
        batch.coalesce(1).write.mode("append").parquet(str(folder))
    return (
        spark.readStream.schema("number long").option("maxFilesPerTrigger", 1).parquet(str(folder))
    )


def failing(rows: DataFrame) -> DataFrame:
    return rows.select(F.raise_error(F.lit("bad record")).alias("failure"))


def start(rows: DataFrame, name: str, tmp_path: Path, *, available_now: bool) -> StreamingQuery:
    """Start a query into a memory table called `name`."""
    writer = (
        rows.writeStream.queryName(name)
        .format("memory")
        .option("checkpointLocation", str(tmp_path / "checkpoints" / name))
    )
    if available_now:
        return writer.trigger(availableNow=True).start()
    return writer.trigger(processingTime="1 second").start()


def no_landed_records(spark: SparkSession, path: str) -> DataFrame:
    """A landing reader that finds nothing, for checks that only need the parsed schemas."""
    return landing_records(spark)
