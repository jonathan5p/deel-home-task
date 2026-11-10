import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.gold.postgres_setup import FakeConnection, StatementFailedError
from analytics.pipelines.tests.gold.silver_setup import (
    CHANGED_AT,
    PRODUCT_VALUES,
    silver_schema,
    silver_version,
)
from common.settings import AnalyticsDbSettings
from pipelines.gold.dimensions import load_dimensions
from pipelines.gold.scd2 import Scd2DefinitionError
from pipelines.gold.sink import Scd2Sink, jdbc_options, staged_rows

PRODUCTS = load_dimensions()["dim_product"]
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

    Scd2Sink(PRODUCTS, stage, lambda: connection)(batch, 7)

    assert log == [
        "stage gold_internal.stage_dim_product",
        *(f"execute {statement}" for statement in PRODUCTS.merge_statements()),
        "commit",
        "close",
    ]


def test_a_failed_merge_fails_the_batch(spark: SparkSession) -> None:
    failing = PRODUCTS.close_statement()
    connection = FakeConnection(fail_on=failing)
    batch = spark.createDataFrame([], silver_schema(PRODUCTS.entity))

    with pytest.raises(StatementFailedError):
        Scd2Sink(PRODUCTS, lambda staged, table: None, lambda: connection)(batch, 7)

    assert connection.log[-2:] == ["rollback", "close"]
