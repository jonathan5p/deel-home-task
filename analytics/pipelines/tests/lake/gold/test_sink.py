import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.silver_setup import (
    CHANGED_AT,
    ITEM_VALUES,
    PRODUCT_VALUES,
    silver_schema,
    silver_version,
)
from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.dimensions import load_dimensions, load_facts
from pipelines.lake.gold.facts import FactDefinitionError
from pipelines.lake.gold.scd2 import Scd2DefinitionError
from pipelines.lake.gold.sink import GoldSink, jdbc_options, staged_rows

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


def test_each_micro_batch_is_handed_to_the_writer_under_its_load_name(
    spark: SparkSession,
) -> None:
    handed: list[tuple[str, DataFrame]] = []

    class Writer:
        def write_batch(self, load: str, batch: DataFrame) -> None:
            handed.append((load, batch))

    batch = spark.createDataFrame([], silver_schema(PRODUCTS.entity))

    GoldSink(PRODUCTS, Writer())(batch, 7)

    assert handed == [("dim_product", batch)]
