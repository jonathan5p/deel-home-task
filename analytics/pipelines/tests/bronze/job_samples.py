import shutil
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType

from analytics.pipelines.tests.bronze.parse_samples import Json, change, landing
from pipelines.bronze.entities import Entity
from pipelines.bronze.job import TableSink, run
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings


def record(entity: Entity, offset: int, value: Json | str | None) -> Row:
    """A Kafka record as the landing job writes it, without the reader's `landing_file`."""
    landed = landing(entity, value, offset=offset)
    return Row(**{name: landed[name] for name in KAFKA_RECORD_SCHEMA.fieldNames()})


def valid(entity: Entity, offset: int) -> Row:
    return record(entity, offset, change(entity, "c"))


def bronze_settings(tmp_path: Path, *topics: str, catalog: str = "catalog") -> BronzeSettings:
    return BronzeSettings.from_env(
        {
            "KAFKA_TOPICS": ",".join(topics),
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": catalog,
        }
    )


def local_iceberg_catalog(spark: SparkSession, warehouse: Path) -> str:
    """Register an Iceberg catalog on local files; the image ships the Iceberg runtime."""
    name = f"iceberg_{warehouse.parent.name.replace('-', '_')}"
    spark.conf.set(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{name}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{name}.warehouse", f"file://{warehouse}")
    return name


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

    def create(self, spark: SparkSession, table: str, schema: StructType) -> None:
        self.schemas[table] = schema

    def sink(self) -> TableSink:
        def append(writer: DataStreamWriter, table: str) -> StreamingQuery:
            return writer.format("parquet").start(str(self.root / table))

        return append

    def rows(self, spark: SparkSession, table: str) -> list[Row]:
        stored = spark.read.schema(self.schemas[table]).parquet(str(self.root / table))
        return sorted(stored.collect(), key=lambda row: (row.record_source, row.source_offset))


def run_until_caught_up(
    spark: SparkSession, settings: BronzeSettings, catalog: FakeCatalog
) -> None:
    queries = run(spark, settings, create=catalog.create, sink=catalog.sink(), available_now=True)
    for query in queries:
        query.awaitTermination()


def offsets(rows: list[Row]) -> list[int]:
    return [row.source_offset for row in rows]
