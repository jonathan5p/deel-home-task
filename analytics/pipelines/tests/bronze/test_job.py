import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

from analytics.pipelines.tests.bronze.job_setup import (
    BronzeSettingsFactory,
    drop_stray_file,
    land,
    local,
    no_landed_records,
    offsets,
    record,
    run_until_caught_up,
    valid,
)
from analytics.pipelines.tests.bronze.parse_samples import ENTITY_IDS, landing_records, topic
from analytics.pipelines.tests.lake.streams_setup import FakeCatalog
from analytics.pipelines.tests.lake.tables_setup import stored_description, table_ddl
from pipelines.bronze.columns import QUARANTINE_COLUMNS, RawColumn
from pipelines.bronze.entities import CUSTOMERS, ENTITIES, PRODUCTS, Entity, UnknownEntityError
from pipelines.bronze.job import BRONZE, QUARANTINE, entity_streams, read_landing, run
from pipelines.bronze.parse import FailureReason, parse_changes
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.streams import await_queries
from pipelines.lake.tables import TABLE_TEMPLATES, described_schema, table_definition
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.render import avro_field, render_table_schema


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_every_column_of_every_bronze_table_is_described(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, entity: Entity
) -> None:
    settings = bronze_settings(topic(entity))

    streams = entity_streams(spark, settings, topic(entity), no_landed_records)

    for stream in streams:
        schema = stream.table_schema()
        assert all(field.metadata["comment"] for field in schema)
        assert schema.fieldNames() == stream.rows.schema.fieldNames()
        assert stream.definition.comment


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_the_published_table_schemas_have_the_columns_and_types_the_job_writes(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, entity: Entity
) -> None:
    settings = bronze_settings(topic(entity))

    bronze, quarantine = entity_streams(spark, settings, topic(entity), no_landed_records)

    for stream, template in (
        (bronze, TABLE_TEMPLATES["bronze"]),
        (quarantine, TABLE_TEMPLATES["bronze_quarantine"]),
    ):
        columns = template.columns_for(entity.definition)
        published = render_table_schema(template, entity.definition)
        assert [(c.lake_name, SPARK_TYPES[c.type]) for c in columns] == [
            (field.name, field.dataType) for field in stream.rows.schema
        ]
        assert published["fields"] == [avro_field(column) for column in columns]
        assert stream.table.endswith(f".{published['name']}")


def test_each_entity_gets_a_bronze_and_a_quarantine_table(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS), topic(PRODUCTS))
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    run_until_caught_up(spark, settings, fake_catalog)

    assert set(fake_catalog.schemas) == {
        "catalog.bronze.customers",
        "catalog.bronze.customers_quarantine",
        "catalog.bronze.products",
        "catalog.bronze.products_quarantine",
    }
    for entity in (CUSTOMERS, PRODUCTS):
        table = f"catalog.bronze.{entity.name}"
        bronze = parse_changes(landing_records(spark), entity).bronze
        assert fake_catalog.schemas[table] == (
            described_schema(table, bronze.schema, table_definition(BRONZE, entity.definition))
        )
        assert fake_catalog.definitions[table] == table_definition(BRONZE, entity.definition)
        quarantine = fake_catalog.schemas[f"{table}_quarantine"]
        assert quarantine.fieldNames() == [*QUARANTINE_COLUMNS, MetadataColumn.QUARANTINED_AT]
        assert fake_catalog.definitions[f"{table}_quarantine"] == table_definition(
            QUARANTINE, entity.definition
        )
    assert quarantine[MetadataColumn.QUARANTINED_AT].dataType == TimestampType()


def test_valid_records_reach_bronze_and_invalid_ones_the_quarantine(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))
    land(
        spark,
        settings,
        [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json"), record(CUSTOMERS, 2, None)],
        "batch-1",
    )

    run_until_caught_up(spark, settings, fake_catalog)

    (bronze,) = fake_catalog.rows(spark, "catalog.bronze.customers")
    assert (
        bronze.customer_id,
        bronze[MetadataColumn.CHANGE_OP],
        bronze[MetadataColumn.SOURCE_OFFSET],
    ) == (7, "create", 0)
    assert MetadataColumn.LANDING_FILE not in bronze.asDict()
    (quarantine,) = fake_catalog.rows(spark, "catalog.bronze.customers_quarantine")
    assert quarantine[MetadataColumn.SOURCE_OFFSET] == 1
    assert quarantine[MetadataColumn.FAILURE_REASON] == FailureReason.NOT_AN_ENVELOPE
    assert quarantine[RawColumn.VALUE] == "not json"
    assert quarantine[MetadataColumn.QUARANTINED_AT] is not None
    assert local(quarantine[MetadataColumn.LANDING_FILE]).parent == local(
        settings.landing_path(topic(CUSTOMERS))
    )
    assert local(quarantine[MetadataColumn.LANDING_FILE]).is_file()


def test_each_stream_keeps_its_own_checkpoint(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")

    run_until_caught_up(spark, settings, fake_catalog)

    checkpoints = local(settings.lake.checkpoint_root) / "bronze"
    assert {path.name for path in checkpoints.iterdir()} == {
        "customers",
        "customers_quarantine",
    }
    assert all((path / "commits").is_dir() for path in checkpoints.iterdir())


def test_a_restart_appends_only_the_records_landed_since(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))
    land(spark, settings, [valid(CUSTOMERS, 0), valid(CUSTOMERS, 1)], "batch-1")
    run_until_caught_up(spark, settings, fake_catalog)

    land(spark, settings, [valid(CUSTOMERS, 2), record(CUSTOMERS, 3, "{}")], "batch-2")
    run_until_caught_up(spark, settings, fake_catalog)
    run_until_caught_up(spark, settings, fake_catalog)

    assert offsets(fake_catalog.rows(spark, "catalog.bronze.customers")) == [0, 1, 2]
    assert offsets(fake_catalog.rows(spark, "catalog.bronze.customers_quarantine")) == [3]


def test_landing_records_are_read_with_their_committed_landing_file(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))
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
    assert {local(landed[MetadataColumn.LANDING_FILE])} == committed


def test_an_unknown_topic_fails_before_any_table_is_created(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS), "test.operations.invoices")
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")

    with pytest.raises(UnknownEntityError, match="invoices"):
        run(spark, settings, create=fake_catalog.create, sink=fake_catalog.sink())

    assert fake_catalog.schemas == {}


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, fake_catalog: FakeCatalog
) -> None:
    settings = bronze_settings(topic(CUSTOMERS), topic(PRODUCTS))
    land(spark, settings, [valid(CUSTOMERS, 0)], "batch-1")
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    queries = run(spark, settings, create=fake_catalog.create, sink=fake_catalog.sink())
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
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
) -> None:
    settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
    land(spark, settings, [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json")], "batch-1")

    def run_on_iceberg() -> None:
        await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)

    run_on_iceberg()
    land(spark, settings, [valid(CUSTOMERS, 2)], "batch-2")
    run_on_iceberg()
    run_on_iceberg()

    bronze = spark.table(f"{iceberg_catalog}.bronze.customers")
    quarantine = spark.table(f"{iceberg_catalog}.bronze.customers_quarantine")
    assert offsets(bronze.orderBy(MetadataColumn.SOURCE_OFFSET).collect()) == [0, 2]
    assert offsets(quarantine.collect()) == [1]
    assert MetadataColumn.LANDING_FILE in quarantine.columns
    assert MetadataColumn.LANDING_FILE not in bronze.columns
    for table in ("customers", "customers_quarantine"):
        ddl = table_ddl(spark, f"{iceberg_catalog}.bronze.{table}")
        assert f"PARTITIONED BY (days({MetadataColumn.PUBLISHED_AT}))" in ddl


def test_iceberg_tables_store_the_table_comment_and_every_column_doc(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
) -> None:
    settings = bronze_settings(topic(PRODUCTS), catalog=iceberg_catalog)
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)

    expected = {
        f"{iceberg_catalog}.bronze.products": table_definition(BRONZE, PRODUCTS.definition),
        f"{iceberg_catalog}.bronze.products_quarantine": table_definition(
            QUARANTINE, PRODUCTS.definition
        ),
    }
    for table, definition in expected.items():
        stored = stored_description(spark, table)
        assert stored.comment == definition.comment
        assert stored.columns == {
            name: definition.columns[name].description for name in stored.columns
        }
