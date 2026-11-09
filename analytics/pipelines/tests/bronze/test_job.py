from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.pipelines.tests.bronze.job_samples import (
    FakeCatalog,
    bronze_settings,
    drop_stray_file,
    land,
    local,
    local_iceberg_catalog,
    offsets,
    record,
    run_until_caught_up,
    valid,
)
from analytics.pipelines.tests.bronze.parse_samples import landing_records, topic
from pipelines.bronze.entities import CUSTOMERS, PRODUCTS, UnknownEntityError
from pipelines.bronze.job import column_definitions, read_landing, run, wants_available_now
from pipelines.bronze.parse import QUARANTINE_COLUMNS, FailureReason, parse_changes


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
    settings = bronze_settings(tmp_path, topic(CUSTOMERS), topic(PRODUCTS))
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    run_until_caught_up(spark, settings, catalog)

    assert set(catalog.schemas) == {
        "catalog.bronze.customers",
        "catalog.bronze.customers_quarantine",
        "catalog.bronze.products",
        "catalog.bronze.products_quarantine",
    }
    for entity in (CUSTOMERS, PRODUCTS):
        bronze = parse_changes(landing_records(spark), entity).bronze
        assert catalog.schemas[f"catalog.bronze.{entity.name}"] == bronze.schema
        quarantine = catalog.schemas[f"catalog.bronze.{entity.name}_quarantine"]
        assert quarantine.fieldNames() == [*QUARANTINE_COLUMNS, "quarantined_at"]
    assert quarantine["quarantined_at"].dataType == TimestampType()


def test_valid_records_reach_bronze_and_invalid_ones_the_quarantine(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, topic(CUSTOMERS))
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
    assert "landing_file" not in bronze.asDict()
    (quarantine,) = catalog.rows(spark, "catalog.bronze.customers_quarantine")
    assert quarantine.source_offset == 1
    assert quarantine.failure_reason == FailureReason.NOT_AN_ENVELOPE
    assert quarantine.value == "not json"
    assert quarantine.quarantined_at is not None
    assert local(quarantine.landing_file).parent == local(settings.landing_path(topic(CUSTOMERS)))
    assert local(quarantine.landing_file).is_file()


def test_each_stream_keeps_its_own_checkpoint(spark: SparkSession, tmp_path: Path) -> None:
    settings = bronze_settings(tmp_path, topic(CUSTOMERS))
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
    settings = bronze_settings(tmp_path, topic(CUSTOMERS))
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0), valid(CUSTOMERS, 1)], "batch-1")
    run_until_caught_up(spark, settings, catalog)

    land(spark, settings, [valid(CUSTOMERS, 2), record(CUSTOMERS, 3, "{}")], "batch-2")
    run_until_caught_up(spark, settings, catalog)
    run_until_caught_up(spark, settings, catalog)

    assert offsets(catalog.rows(spark, "catalog.bronze.customers")) == [0, 1, 2]
    assert offsets(catalog.rows(spark, "catalog.bronze.customers_quarantine")) == [3]


def test_landing_records_are_read_with_their_committed_landing_file(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, topic(CUSTOMERS))
    landing_folder = settings.landing_path(topic(CUSTOMERS))
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    stray = drop_stray_file(spark, settings, [valid(CUSTOMERS, 99)])

    records = read_landing(spark, landing_folder)
    writer = records.writeStream.format("memory").queryName("landed").trigger(availableNow=True)
    writer.start().awaitTermination()

    (landed,) = spark.table("landed").collect()
    committed = set(local(landing_folder).glob("*.avro")) - {stray}
    assert records.isStreaming
    assert landed.offset == 0
    assert {local(landed.landing_file)} == committed


def test_an_unknown_topic_fails_before_any_table_is_created(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, topic(CUSTOMERS), "test.operations.invoices")
    catalog = FakeCatalog(tmp_path / "tables")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")

    with pytest.raises(UnknownEntityError, match="invoices"):
        run(spark, settings, create=catalog.create, sink=catalog.sink())

    assert catalog.schemas == {}


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = bronze_settings(tmp_path, topic(CUSTOMERS), topic(PRODUCTS))
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
    settings = bronze_settings(tmp_path, topic(CUSTOMERS), catalog=catalog)
    land(spark, settings, [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json")], "batch-1")

    def run_on_iceberg() -> None:
        for query in run(spark, settings, available_now=True):
            query.awaitTermination()

    run_on_iceberg()
    land(spark, settings, [valid(CUSTOMERS, 2)], "batch-2")
    run_on_iceberg()
    run_on_iceberg()

    bronze = spark.table(f"{catalog}.bronze.customers")
    quarantine = spark.table(f"{catalog}.bronze.customers_quarantine")
    assert offsets(bronze.orderBy("source_offset").collect()) == [0, 2]
    assert offsets(quarantine.collect()) == [1]
    assert "landing_file" in quarantine.columns
    assert "landing_file" not in bronze.columns
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
