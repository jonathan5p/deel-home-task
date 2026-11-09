from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryManager
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.pipelines.tests.bronze.job_setup import (
    BronzeSettingsFactory,
    FakeCatalog,
    drop_stray_file,
    failing,
    land,
    local,
    no_landed_records,
    numbers,
    offsets,
    record,
    run_until_caught_up,
    start,
    stored_description,
    valid,
)
from analytics.pipelines.tests.bronze.parse_samples import ENTITY_IDS, landing_records, topic
from pipelines.bronze.columns import (
    CHANGE_OP,
    FAILURE_REASON,
    LANDING_FILE,
    PUBLISHED_AT,
    QUARANTINE_COLUMNS,
    QUARANTINED_AT,
    RAW_VALUE,
    SOURCE_OFFSET,
)
from pipelines.bronze.descriptions import (
    TableDescription,
    UndescribedColumnError,
    bronze_table_description,
    quarantine_table_description,
)
from pipelines.bronze.entities import CUSTOMERS, ENTITIES, PRODUCTS, Entity, UnknownEntityError
from pipelines.bronze.job import (
    BronzeStream,
    StreamsFailedError,
    await_queries,
    column_definitions,
    create_table,
    entity_streams,
    read_landing,
    run,
    sql_string,
    table_ddl,
    wants_available_now,
)
from pipelines.bronze.parse import FailureReason, parse_changes

SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("unity_price", DecimalType(38, 10)),
        StructField(PUBLISHED_AT, TimestampType()),
    ]
)
DESCRIPTION = TableDescription(
    comment="Customers",
    columns={"customer_id": "Key", "unity_price": "Price", PUBLISHED_AT: "Published"},
)


def test_column_definitions_keep_the_names_and_spark_types_with_a_comment() -> None:
    assert column_definitions(SCHEMA, DESCRIPTION.columns) == (
        "`customer_id` bigint COMMENT 'Key', `unity_price` decimal(38,10) COMMENT 'Price',"
        f" `{PUBLISHED_AT}` timestamp COMMENT 'Published'"
    )


def test_the_table_ddl_comments_the_table() -> None:
    ddl = table_ddl("catalog.bronze.customers", SCHEMA, DESCRIPTION)

    assert ddl.startswith("CREATE TABLE IF NOT EXISTS catalog.bronze.customers (`customer_id`")
    assert f" USING iceberg PARTITIONED BY (days({PUBLISHED_AT})) COMMENT 'Customers' " in ddl


@pytest.mark.parametrize(
    ("text", "literal"),
    [
        ("plain", "'plain'"),
        ("the customer's name", "'the customer\\'s name'"),
        ("a \\ path", "'a \\\\ path'"),
    ],
)
def test_sql_strings_escape_quotes_and_backslashes(text: str, literal: str) -> None:
    assert sql_string(text) == literal


@pytest.mark.parametrize("blank", ["", "  "])
def test_a_table_with_an_undescribed_column_is_refused(blank: str) -> None:
    description = DESCRIPTION._replace(columns={"customer_id": "Key", "unity_price": blank})

    with pytest.raises(
        UndescribedColumnError, match=rf"customers .*: unity_price, {PUBLISHED_AT}$"
    ):
        table_ddl("catalog.bronze.customers", SCHEMA, description)


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_every_column_of_every_bronze_table_is_described(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, entity: Entity
) -> None:
    settings = bronze_settings(topic(entity))

    streams = entity_streams(spark, settings, topic(entity), no_landed_records)

    for stream in streams:
        assert stream.description.undescribed(stream.rows.schema.fieldNames()) == []
        assert stream.description.comment


def test_a_stream_is_named_after_its_table_and_has_its_own_checkpoint(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))
    rows = landing_records(spark)

    description = quarantine_table_description(CUSTOMERS)

    stream = BronzeStream.for_table(settings, "customers_quarantine", rows, description)

    assert (stream.name, stream.table, stream.checkpoint) == (
        "bronze customers_quarantine",
        "catalog.bronze.customers_quarantine",
        f"{settings.lake.checkpoint_root}/bronze/customers_quarantine",
    )
    assert stream.rows is rows
    assert stream.description is description


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
        bronze = parse_changes(landing_records(spark), entity).bronze
        assert fake_catalog.schemas[f"catalog.bronze.{entity.name}"] == bronze.schema
        quarantine = fake_catalog.schemas[f"catalog.bronze.{entity.name}_quarantine"]
        assert quarantine.fieldNames() == [*QUARANTINE_COLUMNS, QUARANTINED_AT]
        assert fake_catalog.descriptions[f"catalog.bronze.{entity.name}"] == (
            bronze_table_description(entity)
        )
        assert fake_catalog.descriptions[f"catalog.bronze.{entity.name}_quarantine"] == (
            quarantine_table_description(entity)
        )
    assert quarantine[QUARANTINED_AT].dataType == TimestampType()


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
    assert (bronze.customer_id, bronze[CHANGE_OP], bronze[SOURCE_OFFSET]) == (7, "create", 0)
    assert LANDING_FILE not in bronze.asDict()
    (quarantine,) = fake_catalog.rows(spark, "catalog.bronze.customers_quarantine")
    assert quarantine[SOURCE_OFFSET] == 1
    assert quarantine[FAILURE_REASON] == FailureReason.NOT_AN_ENVELOPE
    assert quarantine[RAW_VALUE] == "not json"
    assert quarantine[QUARANTINED_AT] is not None
    assert local(quarantine[LANDING_FILE]).parent == local(settings.landing_path(topic(CUSTOMERS)))
    assert local(quarantine[LANDING_FILE]).is_file()


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
    assert {local(landed[LANDING_FILE])} == committed


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
    assert offsets(bronze.orderBy(SOURCE_OFFSET).collect()) == [0, 2]
    assert offsets(quarantine.collect()) == [1]
    assert LANDING_FILE in quarantine.columns
    assert LANDING_FILE not in bronze.columns
    for table in ("customers", "customers_quarantine"):
        ddl = spark.sql(f"SHOW CREATE TABLE {iceberg_catalog}.bronze.{table}").first()
        assert ddl is not None
        assert f"PARTITIONED BY (days({PUBLISHED_AT}))" in ddl[0]


def test_iceberg_tables_store_the_table_comment_and_every_column_doc(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
) -> None:
    settings = bronze_settings(topic(PRODUCTS), catalog=iceberg_catalog)
    land(spark, settings, [valid(PRODUCTS, 0)], "batch-1")

    await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)

    expected = {
        f"{iceberg_catalog}.bronze.products": bronze_table_description(PRODUCTS),
        f"{iceberg_catalog}.bronze.products_quarantine": quarantine_table_description(PRODUCTS),
    }
    for table, description in expected.items():
        stored = stored_description(spark, table)
        assert stored.comment == description.comment
        assert stored.columns == {name: description.columns[name] for name in stored.columns}


def test_quotes_and_backslashes_in_descriptions_reach_iceberg_verbatim(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.bronze.customers"
    description = TableDescription(
        comment="It's a \\ table",
        columns={name: f"The '{name}' column \\ doc" for name in SCHEMA.fieldNames()},
    )

    create_table(spark, table, SCHEMA, description)

    assert stored_description(spark, table) == (description.comment, description.columns)


def test_a_failed_stream_stops_the_running_ones_and_fails_the_job(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    running = start(numbers(spark, tmp_path / "ok", 1), "running", tmp_path, available_now=False)
    broken = failing(numbers(spark, tmp_path / "bad", 1))
    failed = start(broken, "broken", tmp_path, available_now=False)

    with pytest.raises(StreamsFailedError, match=r"failed: broken$") as raised:
        await_queries(streams, [running, failed], available_now=False)

    assert isinstance(raised.value.__cause__, StreamingQueryException)
    assert not running.isActive
    assert running.exception() is None


def test_a_stream_that_ends_stops_the_running_ones_without_failing(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    rows = numbers(spark, tmp_path / "numbers", 1)
    running = start(rows, "still_running", tmp_path, available_now=False)
    ended = start(rows, "ended", tmp_path, available_now=True)

    await_queries(streams, [running, ended], available_now=False)

    assert not running.isActive
    assert not ended.isActive


def test_an_available_now_run_finishes_every_stream_before_failing(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    broken = failing(numbers(spark, tmp_path / "bad", 1))
    failed = start(broken, "broken_first", tmp_path, available_now=True)
    slow = start(numbers(spark, tmp_path / "ok", 5), "slow", tmp_path, available_now=True)

    with pytest.raises(StreamsFailedError, match=r"failed: broken_first$"):
        await_queries(streams, [failed, slow], available_now=True)

    assert slow.exception() is None
    assert sorted(row.number for row in spark.table("slow").collect()) == [0, 1, 2, 3, 4]


@pytest.mark.parametrize(("argv", "expected"), [([], False), (["--available-now"], True)])
def test_available_now_is_requested_with_a_flag(argv: list[str], expected: bool) -> None:
    assert wants_available_now(argv) is expected


def test_unknown_arguments_are_rejected() -> None:
    with pytest.raises(SystemExit):
        wants_available_now(["--once"])
