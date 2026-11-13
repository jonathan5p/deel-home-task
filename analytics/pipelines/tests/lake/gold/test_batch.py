import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.batch_setup import MemoryGold, batches
from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    ITEMS,
    ORDERS,
    PRODUCTS,
    Values,
    customer,
    item,
    order,
    product,
    rows_of,
    staged,
)
from pipelines.lake.gold.batch import GoldBatches, Write
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.scd2 import Scd2Dimension


def names(writes: list[Write]) -> list[str]:
    return [write.table.name for write in writes]


def rows(writes: list[Write], name: str, *columns: str) -> list[tuple[object, ...]]:
    (write,) = (write for write in writes if write.table.name == name)
    return rows_of(write.rows, *columns)


LOADS: dict[str, Scd2Dimension | ChangeFact] = {
    "dim_customer": CUSTOMERS,
    "dim_product": PRODUCTS,
    "fact_order_history": ORDERS,
    "fact_order_item_history": ITEMS,
}


def written(
    spark: SparkSession, gold: MemoryGold, batch_of: GoldBatches, load: str, changes: list[Values]
) -> list[Write]:
    batch = batch_of.staged(load, staged(spark, LOADS[load], changes))
    return gold.apply(batch_of.writes(load, batch, gold.read))


def test_a_customer_batch_writes_only_its_versions(spark: SparkSession) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)

    writes = written(spark, gold, batch_of, "dim_customer", [customer(1, 10, "Ada")])

    assert names(writes) == ["dim_customer"]
    assert gold.reads == ["dim_customer"]
    hashed = F.xxhash64(F.lit(1).cast("long"), F.lit(10).cast("long"))
    ((key,),) = spark.range(1).select(hashed).collect()
    assert rows(writes, "dim_customer", "customer_id", "customer_key", "_is_current") == [
        (1, key, True)
    ]


def test_a_product_batch_writes_only_its_versions(spark: SparkSession) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)

    writes = written(spark, gold, batch_of, "dim_product", [product(1, 10, "2.50")])

    assert names(writes) == ["dim_product"]


def test_an_order_batch_writes_its_history_and_current_state_from_its_own_tables(
    spark: SparkSession,
) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)
    changes = [order(7, 10), order(7, 20, "PROCESSING"), order(8, 30)]

    writes = written(spark, gold, batch_of, "fact_order_history", changes)

    assert names(writes) == ["fact_order_history", "current_orders"]
    assert sorted(set(gold.reads)) == ["current_orders", "fact_order_history"]
    assert rows(writes, "fact_order_history", "order_id", "_commit_sequence", "status_key") == [
        (7, 10, 1),
        (7, 20, 2),
        (8, 30, 1),
    ]
    assert rows(writes, "current_orders", "order_id", "status_key", "is_open") == [
        (7, 2, True),
        (8, 1, True),
    ]


def test_an_item_batch_writes_its_history_and_current_state(spark: SparkSession) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)
    changes = [item(5, 20, quantity=3), item(6, 30, product_id=2)]

    writes = written(spark, gold, batch_of, "fact_order_item_history", changes)

    assert names(writes) == ["fact_order_item_history", "current_order_items"]
    assert rows(writes, "current_order_items", "order_item_id", "product_id", "quantity") == [
        (5, 1, 3),
        (6, 2, 2),
    ]


def test_a_late_customer_writes_nothing_to_the_orders(spark: SparkSession) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)
    written(spark, gold, batch_of, "fact_order_history", [order(7, 20)])
    orders = gold.read("fact_order_history").collect()

    writes = written(spark, gold, batch_of, "dim_customer", [customer(1, 10, "Ada")])

    assert names(writes) == ["dim_customer"]
    assert gold.read("fact_order_history").collect() == orders


def test_a_replayed_batch_writes_nothing(spark: SparkSession) -> None:
    gold, batch_of = MemoryGold(spark), batches(spark)
    written(spark, gold, batch_of, "fact_order_history", [order(7, 10)])

    writes = written(spark, gold, batch_of, "fact_order_history", [order(7, 10)])

    assert rows(writes, "fact_order_history", "order_id") == []
    assert rows(writes, "current_orders", "order_id") == []


def test_each_load_writes_only_its_own_tables(spark: SparkSession) -> None:
    batch_of = batches(spark)

    assert batch_of.tables_of("dim_customer") == ("dim_customer",)
    assert batch_of.tables_of("fact_order_item_history") == (
        "fact_order_item_history",
        "current_order_items",
    )


def test_a_dimension_batch_is_staged_as_it_comes(spark: SparkSession) -> None:
    batch = staged(spark, CUSTOMERS, [customer(1, 10, "Ada")])

    assert batches(spark).staged("dim_customer", batch) is batch
