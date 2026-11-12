from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.gold.silver_setup import (
    CHANGED_AT,
    ITEM_VALUES,
    PRODUCT_VALUES,
    silver_schema,
    silver_version,
    write_silver,
)
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, load_dimensions, load_facts
from pipelines.lake.gold.job import SilverReader
from pipelines.schemas.definitions import EntityDefinition

CUSTOMERS = load_dimensions()["dim_customer"]
PRODUCTS = load_dimensions()["dim_product"]
ITEMS = load_facts()["fact_order_item_history"]
ORDERS = load_facts()["fact_order_history"]
ENTITIES = [load.entity for load in (*DIMENSIONS, *FACTS)]
LATER = CHANGED_AT + timedelta(hours=1)
DATABASE = {
    "LAKE_ROOT": "file:///lake",
    "ICEBERG_CATALOG": "catalog",
    "ANALYTICS_DB_URL": "jdbc:postgresql://db/gold",
    "ANALYTICS_DB_USER": "gold",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def write_silver_tables(spark: SparkSession, root: Path) -> None:
    """One version per entity at CHANGED_AT, and a later product and order version."""
    write_silver(
        spark, root / "customers", CUSTOMERS.entity, [silver_version(CUSTOMERS.entity, 7, 10)]
    )
    write_silver(
        spark,
        root / "products",
        PRODUCTS.entity,
        [
            silver_version(PRODUCTS.entity, 3, 20, **PRODUCT_VALUES),
            silver_version(PRODUCTS.entity, 3, 90, _changed_at=LATER, **PRODUCT_VALUES),
        ],
    )
    write_silver(
        spark,
        root / "orders",
        ORDERS.entity,
        [
            silver_version(ORDERS.entity, 70, 50),
            silver_version(ORDERS.entity, 70, 95, _changed_at=LATER),
        ],
    )
    item = silver_version(ITEMS.entity, 9, 60, **ITEM_VALUES)
    write_silver(spark, root / "order_items", ITEMS.entity, [item])


def silver_tables(root: Path, entities: list[EntityDefinition], log: list[str]) -> SilverReader:
    """A reader that reads each silver table in batch from a parquet folder per entity under
    `root`, recording the tables it was asked for."""
    schemas = {entity.name: silver_schema(entity) for entity in entities}

    def read(spark: SparkSession, table: str) -> DataFrame:
        log.append(f"read {table}")
        entity = table.rsplit(".", 1)[-1]
        return spark.read.schema(schemas[entity]).parquet(str(root / entity))

    return read
