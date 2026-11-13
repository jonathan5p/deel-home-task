from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryManager

from analytics.pipelines.tests.lake.gold.silver_setup import (
    ITEM_VALUES,
    PRODUCT_VALUES,
    RecordingSinks,
    silver_folders,
    silver_schema,
    silver_version,
    write_silver,
)
from analytics.pipelines.tests.lake.gold.writer_setup import FakeLake, FakeServing
from pipelines.lake.gold.batch import GoldBatches
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED, load_dimensions, load_facts
from pipelines.lake.gold.job import (
    gold_stream,
    iceberg_gold,
    postgres_serving,
    prepare,
    run,
    seeded_rows,
    session_conf,
    writer_sinks,
)
from pipelines.lake.gold.sink import GoldSink
from pipelines.lake.gold.writer import GoldWriter
from pipelines.lake.shared.streams import await_queries
from pipelines.settings import GoldSettings

CUSTOMERS = load_dimensions()["dim_customer"]
PRODUCTS = load_dimensions()["dim_product"]
ITEMS = load_facts()["fact_order_item_history"]
ORDERS = load_facts()["fact_order_history"]
ENTITIES = [load.entity for load in (*DIMENSIONS, *FACTS)]


@pytest.fixture
def settings(tmp_path: Path) -> GoldSettings:
    return GoldSettings.from_env(
        {
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
            "ANALYTICS_DB_URL": "jdbc:postgresql://db/gold",
            "ANALYTICS_DB_USER": "gold",
            "ANALYTICS_DB_PASSWORD": "secret",
            "GOLD_TRIGGER_SECONDS": "1",
        }
    )


@pytest.fixture
def iceberg_settings(settings: GoldSettings, iceberg_catalog: str) -> GoldSettings:
    return replace(settings, lake=replace(settings.lake, catalog=iceberg_catalog))


@pytest.fixture
def silver(spark: SparkSession, tmp_path: Path) -> Path:
    root = tmp_path / "silver"
    write_silver(
        spark, root / "customers", CUSTOMERS.entity, [silver_version(CUSTOMERS.entity, 7, 10)]
    )
    write_silver(
        spark,
        root / "products",
        PRODUCTS.entity,
        [
            silver_version(PRODUCTS.entity, 3, 20, **PRODUCT_VALUES),
            silver_version(PRODUCTS.entity, 3, 30, **PRODUCT_VALUES),
        ],
    )
    write_silver(spark, root / "orders", ORDERS.entity, [silver_version(ORDERS.entity, 70, 50)])
    write_silver(
        spark,
        root / "order_items",
        ITEMS.entity,
        [silver_version(ITEMS.entity, 9, 60, **ITEM_VALUES)],
    )
    return root


def test_each_load_streams_its_silver_table_into_its_own_checkpoint(
    spark: SparkSession, settings: GoldSettings, silver: Path, tmp_path: Path
) -> None:
    read_tables: list[str] = []
    folders = silver_folders(silver, ENTITIES)

    def read(session: SparkSession, table: str) -> DataFrame:
        read_tables.append(table)
        return folders(session, table)

    dimension = gold_stream(spark, settings, PRODUCTS, read, RecordingSinks())
    fact = gold_stream(spark, settings, ITEMS, read, RecordingSinks())

    assert read_tables == ["catalog.silver.products", "catalog.silver.order_items"]
    assert dimension.name == "gold dim_product"
    assert dimension.checkpoint == f"file://{tmp_path}/lake/checkpoints/gold/dim_product"
    assert dimension.rows.columns == list(PRODUCTS.silver_columns)
    assert fact.name == "gold fact_order_item_history"
    assert fact.checkpoint == (f"file://{tmp_path}/lake/checkpoints/gold/fact_order_item_history")
    assert fact.rows.columns == list(ITEMS.silver_columns)


def test_an_available_now_run_hands_every_silver_version_to_its_sink(
    spark: SparkSession, streams: StreamingQueryManager, settings: GoldSettings, silver: Path
) -> None:
    sinks = RecordingSinks()

    queries = run(
        spark, settings, read=silver_folders(silver, ENTITIES), sink=sinks, available_now=True
    )
    await_queries(streams, queries, available_now=True)

    assert [query.name for query in queries] == [
        "gold dim_customer",
        "gold dim_product",
        "gold fact_order_history",
        "gold fact_order_item_history",
    ]
    assert [[row.customer_id for row in batch] for batch in sinks.batches["dim_customer"]] == [[7]]
    (products,) = sinks.batches["dim_product"]
    assert sorted(row._commit_sequence for row in products) == [20, 30]
    assert products[0].unit_price == PRODUCT_VALUES["unity_price"]
    assert [[row.order_id for row in batch] for batch in sinks.batches["fact_order_history"]] == [
        [70]
    ]
    (items,) = sinks.batches["fact_order_item_history"]
    assert [row.quantity for row in items] == [ITEM_VALUES["quantity"]]
    checkpoints = Path(settings.lake.checkpoint_root.removeprefix("file://")) / "gold"
    assert sorted(path.name for path in checkpoints.iterdir()) == [
        "dim_customer",
        "dim_product",
        "fact_order_history",
        "fact_order_item_history",
    ]


def test_a_restart_hands_over_only_the_versions_added_since(
    spark: SparkSession, streams: StreamingQueryManager, settings: GoldSettings, silver: Path
) -> None:
    read = silver_folders(silver, ENTITIES)
    first = run(spark, settings, read=read, sink=RecordingSinks(), available_now=True)
    await_queries(streams, first, available_now=True)
    write_silver(
        spark, silver / "customers", CUSTOMERS.entity, [silver_version(CUSTOMERS.entity, 7, 40)]
    )
    sinks = RecordingSinks()

    queries = run(spark, settings, read=read, sink=sinks, available_now=True)
    await_queries(streams, queries, available_now=True)

    assert [[row._commit_sequence for row in batch] for batch in sinks.batches["dim_customer"]] == [
        [40]
    ]
    assert set(sinks.batches) == {"dim_customer"}


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, settings: GoldSettings, silver: Path
) -> None:
    queries = run(spark, settings, read=silver_folders(silver, ENTITIES), sink=RecordingSinks())
    try:
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()


def test_each_load_hands_its_batches_to_the_shared_writer(
    spark: SparkSession, settings: GoldSettings
) -> None:
    batches = GoldBatches(STORED, DIMENSIONS, FACTS, seeded_rows(spark))
    writer = GoldWriter(batches, FakeLake({}, []), None)

    sink = writer_sinks(writer)(PRODUCTS)

    assert sink == GoldSink(PRODUCTS, writer)


def test_the_gold_tables_are_named_in_the_gold_namespace_apart_from_its_views(
    spark: SparkSession, settings: GoldSettings
) -> None:
    lake = iceberg_gold(spark, settings)

    assert lake.relation["dim_customer"] == "catalog.gold.internal_dim_customer"
    assert set(lake.relation) == set(STORED)


def test_a_gold_session_shares_its_executors_fairly_and_never_caches_the_catalog_tables(
    settings: GoldSettings,
) -> None:
    assert session_conf(settings) == {
        "spark.sql.shuffle.partitions": "4",
        "spark.scheduler.mode": "FAIR",
        "spark.sql.catalog.catalog.cache-enabled": "false",
    }


def test_the_copy_keeps_history_from_the_retention_window_of_today(settings: GoldSettings) -> None:
    serving = postgres_serving(replace(settings, retention_months=36), lambda: date(2026, 11, 12))

    assert serving.retained_from() == datetime(2023, 11, 1)


def test_the_job_writes_gold_to_iceberg_and_copies_each_batch(
    spark: SparkSession,
    streams: StreamingQueryManager,
    iceberg_settings: GoldSettings,
    silver: Path,
) -> None:
    for key, value in session_conf(iceberg_settings, {}).items():
        spark.conf.set(key, value)
    log: list[str] = []
    serving = FakeServing(log)
    lake = iceberg_gold(spark, iceberg_settings)
    writer = GoldWriter(GoldBatches(STORED, DIMENSIONS, FACTS, seeded_rows(spark)), lake, serving)

    prepare(spark, writer, lake)
    queries = run(
        spark,
        iceberg_settings,
        writer_sinks(writer),
        read=silver_folders(silver, ENTITIES),
        available_now=True,
    )
    await_queries(streams, queries, available_now=True)

    assert lake.read("dim_order_status").count() == 5
    assert lake.read("dim_date").count() == 36525
    assert [row.product_id for row in lake.read("dim_product").collect()] == [3, 3]
    items = lake.read("current_order_items").collect()
    assert [(row.product_id, row.quantity) for row in items] == [(3, 2)]
    assert serving.synced_snapshots == {name: lake.snapshot(name) for name in STORED}
    assert len(log) >= 5


def test_silver_is_streamed_from_its_iceberg_table(
    spark: SparkSession, streams: StreamingQueryManager, iceberg_settings: GoldSettings
) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {iceberg_settings.lake.catalog}.silver")
    versions = {
        CUSTOMERS.entity: silver_version(CUSTOMERS.entity, 5, 10),
        PRODUCTS.entity: silver_version(PRODUCTS.entity, 5, 10, **PRODUCT_VALUES),
        ORDERS.entity: silver_version(ORDERS.entity, 5, 10),
        ITEMS.entity: silver_version(ITEMS.entity, 5, 10, **ITEM_VALUES),
    }
    for entity, version in versions.items():
        rows = spark.createDataFrame([version], silver_schema(entity))
        rows.writeTo(iceberg_settings.source_table(entity.name)).create()
    sinks = RecordingSinks()

    queries = run(spark, iceberg_settings, sink=sinks, available_now=True)
    await_queries(streams, queries, available_now=True)

    assert [[row._commit_sequence for row in batch] for batch in sinks.batches["dim_product"]] == [
        [10]
    ]
