import json
import shutil
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.bronze.entities import CUSTOMERS, PRODUCTS, Entity, UnknownEntityError
from pipelines.bronze.job import (
    TableSink,
    column_definitions,
    read_landing,
    run,
    wants_available_now,
)
from pipelines.bronze.parse import QUARANTINE_COLUMNS, FailureReason, parse_changes
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import BronzeSettings

PUBLISHED = datetime(2026, 11, 8, 14, 21, 1)  # Spark returns naive timestamps in UTC
TOPICS = {CUSTOMERS: "test.operations.customers", PRODUCTS: "test.operations.products"}
ROWS: dict[Entity, dict[str, object]] = {
    CUSTOMERS: {"customer_id": 7, "customer_name": "ACME INC", "is_active": True},
    PRODUCTS: {
        "product_id": 3,
        "product_name": "Notebook",
        "barcode": "7701234567890",
        "unity_price": {"scale": 2, "value": "AJk="},
    },
}


def change(entity: Entity, lsn: int) -> str:
    return json.dumps(
        {
            "before": None,
            "after": ROWS[entity],
            "op": "c",
            "ts_ms": 1794147660561,
            "source": {"lsn": lsn, "ts_ms": 1794147660025},
        }
    )


def record(entity: Entity, offset: int, value: str | None) -> Row:
    return Row(
        key=json.dumps({entity.key: offset}).encode(),
        value=None if value is None else value.encode(),
        topic=TOPICS[entity],
        partition=0,
        offset=offset,
        timestamp=PUBLISHED,
        timestampType=0,
    )


def valid(entity: Entity, offset: int) -> Row:
    return record(entity, offset, change(entity, lsn=100 + offset))


def bronze_settings(tmp_path: Path, *entities: Entity, catalog: str = "catalog") -> BronzeSettings:
    return BronzeSettings.from_env(
        {
            "KAFKA_TOPICS": ",".join(TOPICS[entity] for entity in entities),
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
    return Path(path.removeprefix("file://"))


def land(spark: SparkSession, settings: BronzeSettings, rows: list[Row], batch: str) -> None:
    """Land records like the landing job: through a file sink, which keeps a commit log."""
    topic = rows[0].topic
    staging = local(settings.lake.root) / "staging" / topic
    spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).write.parquet(str(staging / batch))
    query = (
        spark.readStream.schema(KAFKA_RECORD_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(str(staging))
        .writeStream.format("avro")
        .option("path", settings.landing_path(topic))
        .option("checkpointLocation", f"{settings.lake.checkpoint_root}/landing/{topic}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def drop_stray_file(spark: SparkSession, settings: BronzeSettings, rows: list[Row]) -> None:
    """Leave an Avro file in a landing folder without recording it in the commit log."""
    topic = rows[0].topic
    elsewhere = local(settings.lake.root) / "stray"
    spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA).coalesce(1).write.format("avro").save(
        str(elsewhere)
    )
    (avro,) = elsewhere.glob("*.avro")
    shutil.move(avro, local(settings.landing_path(topic)) / "part-stray.avro")


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


def test_column_definitions_keep_the_names_and_spark_types() -> None:
    schema = StructType(
        [
            StructField("customer_id", LongType()),
            StructField("unity_price", DecimalType(38, 10)),
            StructField("published_at", TimestampType()),
            StructField("record_source", StringType()),
        ]
    )

    assert column_definitions(schema) == (
        "`customer_id` bigint, `unity_price` decimal(38,10),"
        " `published_at` timestamp, `record_source` string"
    )


def test_each_entity_gets_a_bronze_and_a_quarantine_table(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS, PRODUCTS)
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    run_until_caught_up(spark, settings, catalog)

    landing_records = spark.createDataFrame([], KAFKA_RECORD_SCHEMA)
    assert set(catalog.schemas) == {
        "catalog.bronze.customers",
        "catalog.bronze.customers_quarantine",
        "catalog.bronze.products",
        "catalog.bronze.products_quarantine",
    }
    for entity in (CUSTOMERS, PRODUCTS):
        bronze = parse_changes(landing_records, entity).bronze
        assert catalog.schemas[f"catalog.bronze.{entity.name}"] == bronze.schema
        quarantine = catalog.schemas[f"catalog.bronze.{entity.name}_quarantine"]
        assert quarantine.fieldNames() == [*QUARANTINE_COLUMNS, "quarantined_at"]
    assert quarantine["quarantined_at"].dataType == TimestampType()


def test_valid_records_reach_bronze_and_invalid_ones_the_quarantine(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS)
    catalog = FakeCatalog(tmp_path / "tables")
    land(
        spark,
        settings,
        [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json"), record(CUSTOMERS, 2, None)],
        "batch-1",
    )

    run_until_caught_up(spark, settings, catalog)

    (bronze,) = catalog.rows(spark, "catalog.bronze.customers")
    assert (bronze.customer_id, bronze.change_op, bronze.source_offset) == (7, "create", 0)
    (quarantine,) = catalog.rows(spark, "catalog.bronze.customers_quarantine")
    assert quarantine.source_offset == 1
    assert quarantine.failure_reason == FailureReason.NOT_AN_ENVELOPE
    assert quarantine.value == "not json"
    assert quarantine.quarantined_at is not None


def test_each_stream_keeps_its_own_checkpoint(spark: SparkSession, tmp_path: Path) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS)
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")

    run_until_caught_up(spark, settings, catalog)

    checkpoints = local(settings.lake.checkpoint_root) / "bronze"
    assert {path.name for path in checkpoints.iterdir()} == {
        "customers",
        "customers_quarantine",
    }
    assert all((path / "commits").is_dir() for path in checkpoints.iterdir())


def test_a_restart_appends_only_the_records_landed_since(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS)
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0), valid(CUSTOMERS, 1)], "batch-1")
    run_until_caught_up(spark, settings, catalog)

    land(spark, settings, [valid(CUSTOMERS, 2), record(CUSTOMERS, 3, "{}")], "batch-2")
    run_until_caught_up(spark, settings, catalog)
    run_until_caught_up(spark, settings, catalog)

    assert offsets(catalog.rows(spark, "catalog.bronze.customers")) == [0, 1, 2]
    assert offsets(catalog.rows(spark, "catalog.bronze.customers_quarantine")) == [3]


def test_files_outside_the_landing_commit_log_are_never_read(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS)
    landing_folder = settings.landing_path(TOPICS[CUSTOMERS])
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    drop_stray_file(spark, settings, [valid(CUSTOMERS, 99)])

    records = read_landing(spark, landing_folder)
    writer = records.writeStream.format("memory").queryName("landed").trigger(availableNow=True)
    writer.start().awaitTermination()

    assert records.isStreaming
    assert [row.offset for row in spark.table("landed").collect()] == [0]
    assert len(list(local(landing_folder).glob("*.avro"))) == 2


def test_an_unknown_topic_fails_before_any_table_is_created(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = BronzeSettings.from_env(
        {
            "KAFKA_TOPICS": f"{TOPICS[CUSTOMERS]},test.operations.invoices",
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
        }
    )
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")

    with pytest.raises(UnknownEntityError, match="invoices"):
        run(spark, settings, create=catalog.create, sink=catalog.sink())

    assert catalog.schemas == {}


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, CUSTOMERS, PRODUCTS)
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    queries = run(spark, settings, create=catalog.create, sink=catalog.sink())
    try:
        assert [query.name for query in queries] == [
            "bronze customers",
            "bronze customers_quarantine",
            "bronze products",
            "bronze products_quarantine",
        ]
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()


def test_bronze_lands_in_day_partitioned_iceberg_tables_exactly_once(
    spark: SparkSession, tmp_path: Path
) -> None:
    catalog = local_iceberg_catalog(spark, tmp_path / "warehouse")
    settings = bronze_settings(tmp_path, CUSTOMERS, catalog=catalog)
    land(spark, settings, [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json")], "batch-1")

    def run_on_iceberg() -> None:
        for query in run(spark, settings, available_now=True):
            query.awaitTermination()

    run_on_iceberg()
    land(spark, settings, [valid(CUSTOMERS, 2)], "batch-2")
    run_on_iceberg()
    run_on_iceberg()

    bronze = spark.table(f"{catalog}.bronze.customers").orderBy("source_offset").collect()
    quarantine = spark.table(f"{catalog}.bronze.customers_quarantine").collect()
    assert offsets(bronze) == [0, 2]
    assert offsets(quarantine) == [1]
    for table in ("customers", "customers_quarantine"):
        ddl = spark.sql(f"SHOW CREATE TABLE {catalog}.bronze.{table}").first()
        assert ddl is not None
        assert "PARTITIONED BY (days(published_at))" in ddl[0]


@pytest.mark.parametrize(("argv", "expected"), [([], False), (["--available-now"], True)])
def test_available_now_is_requested_with_a_flag(argv: list[str], expected: bool) -> None:
    assert wants_available_now(argv) is expected


def test_unknown_arguments_are_rejected() -> None:
    with pytest.raises(SystemExit):
        wants_available_now(["--once"])
