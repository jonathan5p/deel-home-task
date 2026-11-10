import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.gold.postgres_setup import FakeConnection, StatementFailedError
from analytics.pipelines.tests.gold.silver_setup import (
    CHANGED_AT,
    ITEM_VALUES,
    PRODUCT_VALUES,
    silver_schema,
    silver_version,
)
from common.settings import AnalyticsDbSettings
from pipelines.gold.dimensions import FACTS, load_dimensions, load_facts
from pipelines.gold.facts import FactDefinitionError
from pipelines.gold.postgres import WRITER_LOCK
from pipelines.gold.scd2 import Scd2DefinitionError
from pipelines.gold.sink import MergeSink, jdbc_options, staged_rows

PRODUCTS = load_dimensions()["dim_product"]
ITEMS = load_facts()["fact_order_item_history"]
SETTINGS = AnalyticsDbSettings(url="jdbc:postgresql://db/gold", user="gold", password="secret")


def test_silver_rows_are_staged_under_their_gold_names(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [silver_version(PRODUCTS.entity, 3, 101, **PRODUCT_VALUES)], silver_schema(PRODUCTS.entity)
    )

    (row,) = staged_rows(silver, PRODUCTS).collect()

    assert row.asDict() == {
        "product_id": 3,
        "product_name": "Notebook",
        "barcode": None,
        "unit_price": PRODUCT_VALUES["unity_price"],
        "is_active": None,
        "_commit_sequence": 101,
        "_content_hash": "hash-3-101",
        "_is_deleted": False,
        "_valid_from": CHANGED_AT,
    }


def test_silver_rows_without_a_needed_column_are_refused(spark: SparkSession) -> None:
    silver = spark.createDataFrame([], silver_schema(PRODUCTS.entity)).drop("unity_price")

    with pytest.raises(Scd2DefinitionError, match="no columns named unity_price"):
        staged_rows(silver, PRODUCTS)


def test_silver_changes_are_staged_for_their_history_fact(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [silver_version(ITEMS.entity, 9, 205, **ITEM_VALUES)], silver_schema(ITEMS.entity)
    )

    (row,) = staged_rows(silver, ITEMS).collect()

    assert row.asDict() == {
        "order_item_id": 9,
        "_commit_sequence": 205,
        "_changed_at": CHANGED_AT,
        "_change_op": "update",
        "order_id": 70,
        "product_id": 3,
        "quantity": 2,
        "_content_hash": "hash-9-205",
        "_is_deleted": False,
    }


def test_silver_changes_without_a_needed_column_are_refused(spark: SparkSession) -> None:
    silver = spark.createDataFrame([], silver_schema(ITEMS.entity)).drop("_content_hash")

    with pytest.raises(FactDefinitionError, match="no columns named _content_hash"):
        staged_rows(silver, ITEMS)


def test_the_staging_table_is_emptied_and_reloaded_but_kept() -> None:
    assert jdbc_options(SETTINGS, "gold_internal.stage_dim_product") == {
        "url": "jdbc:postgresql://db/gold",
        "user": "gold",
        "password": "secret",
        "driver": "org.postgresql.Driver",
        "dbtable": "gold_internal.stage_dim_product",
        "truncate": "true",
    }


def test_a_batch_is_staged_then_merged_in_one_transaction(spark: SparkSession) -> None:
    log: list[str] = []
    connection = FakeConnection(log=log)
    batch = spark.createDataFrame([], silver_schema(PRODUCTS.entity))

    def stage(staged: DataFrame, table: str) -> None:
        assert staged is batch
        log.append(f"stage {table}")

    MergeSink(PRODUCTS, stage, lambda: connection)(batch, 7)

    assert log == [
        "stage gold_internal.stage_dim_product",
        f"execute {WRITER_LOCK}",
        *(f"execute {statement}" for statement in PRODUCTS.merge_statements()),
        "commit",
        "close",
    ]


def test_every_merge_takes_the_gold_writer_lock_first() -> None:
    sinks = [MergeSink(load, lambda staged, table: None, FakeConnection) for load in FACTS]

    assert all(sink.statements()[0] == WRITER_LOCK for sink in sinks)


def test_a_dimension_merge_is_followed_by_the_re_resolution_of_its_facts() -> None:
    sink = MergeSink(PRODUCTS, lambda staged, table: None, FakeConnection, FACTS)

    assert sink.statements() == (
        WRITER_LOCK,
        *PRODUCTS.merge_statements(),
        *ITEMS.resolve_statements("dim_product"),
    )


def test_a_fact_merge_re_resolves_no_other_fact() -> None:
    sink = MergeSink(ITEMS, lambda staged, table: None, FakeConnection, FACTS)

    assert sink.statements() == (WRITER_LOCK, *ITEMS.merge_statements())


def test_a_failed_merge_fails_the_batch(spark: SparkSession) -> None:
    failing = PRODUCTS.insert_statement()
    connection = FakeConnection(fail_on=failing)
    batch = spark.createDataFrame([], silver_schema(PRODUCTS.entity))

    with pytest.raises(StatementFailedError):
        MergeSink(PRODUCTS, lambda staged, table: None, lambda: connection)(batch, 7)

    assert connection.log[-2:] == ["rollback", "close"]
