import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.batch_setup import MemoryGold
from analytics.pipelines.tests.lake.gold.model.model_samples import (
    CUSTOMERS,
    DELIVERY_DATE,
    ITEMS,
    ORDERS,
    PRODUCTS,
    customer,
    item,
    order,
    product,
    rows_of,
    staged,
)
from pipelines.lake.gold.batch import GoldBatches, Write, merged, recounted, with_resolved
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED

BATCHES = GoldBatches(STORED, DIMENSIONS, FACTS)


def names(writes: list[Write]) -> list[str]:
    return [write.table.name for write in writes]


def rows(writes: list[Write], name: str, *columns: str) -> list[tuple[object, ...]]:
    (write,) = (write for write in writes if write.table.name == name)
    return rows_of(write.rows, *columns)


def test_a_customer_batch_writes_its_versions_then_the_orders_and_the_top_customers(
    spark: SparkSession,
) -> None:
    gold = MemoryGold(spark)
    batch = staged(spark, CUSTOMERS, [customer(1, 10, "Ada")])

    writes = gold.apply(BATCHES.writes("dim_customer", batch, gold.read))

    assert names(writes) == [
        "dim_customer",
        "fact_order_history",
        "top_customers_by_pending_orders",
    ]
    hashed = F.xxhash64(F.lit(1).cast("long"), F.lit(10).cast("long"))
    ((key,),) = spark.range(1).select(hashed).collect()
    assert rows(writes, "dim_customer", "customer_id", "customer_key", "_is_current") == [
        (1, key, True)
    ]


def test_a_product_batch_writes_its_versions_then_the_items_it_re_resolves(
    spark: SparkSession,
) -> None:
    gold = MemoryGold(spark)
    batch = staged(spark, PRODUCTS, [product(1, 10, "2.50")])

    writes = gold.apply(BATCHES.writes("dim_product", batch, gold.read))

    assert names(writes) == ["dim_product", "fact_order_item_history"]


def test_an_order_batch_writes_history_and_current_state_then_recounts_and_ranks_the_reports(
    spark: SparkSession,
) -> None:
    gold = MemoryGold(spark)
    batch = staged(spark, ORDERS, [order(7, 10), order(7, 20, "PROCESSING"), order(8, 30)])

    writes = gold.apply(BATCHES.writes("fact_order_history", batch, gold.read))

    assert names(writes) == [
        "fact_order_history",
        "current_orders",
        "open_orders_by_delivery_date_and_status",
        "pending_orders_by_customer",
        "pending_items_by_product",
        "top_delivery_dates_by_open_orders",
        "top_customers_by_pending_orders",
    ]
    assert rows(writes, "fact_order_history", "order_id", "_commit_sequence", "_is_current") == [
        (7, 10, False),
        (7, 20, True),
        (8, 30, True),
    ]
    assert rows(writes, "current_orders", "order_id", "status_key") == [(7, 2), (8, 1)]
    assert rows(
        writes, "open_orders_by_delivery_date_and_status", "status_code", "open_orders"
    ) == [("PENDING", 1), ("PROCESSING", 1)]
    assert rows(writes, "pending_orders_by_customer", "customer_id", "pending_orders") == [(1, 2)]
    assert rows(writes, "pending_items_by_product", "product_id") == []
    assert rows(writes, "top_delivery_dates_by_open_orders", "delivery_date", "open_orders") == [
        (DELIVERY_DATE, 2)
    ]
    assert rows(writes, "top_customers_by_pending_orders", "customer_id", "pending_orders") == [
        (1, 2)
    ]


def test_closing_an_order_recounts_its_old_keys_to_removed(spark: SparkSession) -> None:
    gold = MemoryGold(spark)
    gold.apply(
        BATCHES.writes("fact_order_history", staged(spark, ORDERS, [order(7, 10)]), gold.read)
    )

    closed = staged(spark, ORDERS, [order(7, 20, "COMPLETED")])
    writes = gold.apply(BATCHES.writes("fact_order_history", closed, gold.read))

    assert rows(writes, "pending_orders_by_customer", "customer_id", "_is_removed") == [(1, True)]
    assert rows_of(gold.read("pending_orders_by_customer"), "customer_id") == []
    assert rows_of(gold.read("top_customers_by_pending_orders"), "customer_id") == []


def test_an_item_batch_recounts_the_pending_items_of_its_products(spark: SparkSession) -> None:
    gold = MemoryGold(spark)
    gold.apply(
        BATCHES.writes("fact_order_history", staged(spark, ORDERS, [order(1, 10)]), gold.read)
    )
    batch = staged(spark, ITEMS, [item(5, 20, quantity=3), item(6, 30, product_id=2)])

    writes = gold.apply(BATCHES.writes("fact_order_item_history", batch, gold.read))

    assert names(writes) == [
        "fact_order_item_history",
        "current_order_items",
        "pending_items_by_product",
    ]
    assert rows(
        writes, "pending_items_by_product", "product_id", "pending_items", "pending_quantity"
    ) == [
        (1, 1, 3),
        (2, 1, 2),
    ]


def test_an_order_arriving_after_its_items_recounts_their_products(spark: SparkSession) -> None:
    gold = MemoryGold(spark)
    gold.apply(
        BATCHES.writes("fact_order_item_history", staged(spark, ITEMS, [item(5, 10)]), gold.read)
    )

    batch = staged(spark, ORDERS, [order(1, 20)])
    gold.apply(BATCHES.writes("fact_order_history", batch, gold.read))

    assert rows_of(gold.read("pending_items_by_product"), "product_id", "pending_items") == [(1, 1)]


def test_a_late_customer_re_resolves_its_orders_and_names_the_top_customer(
    spark: SparkSession,
) -> None:
    gold = MemoryGold(spark)
    gold.apply(
        BATCHES.writes("fact_order_history", staged(spark, ORDERS, [order(7, 20)]), gold.read)
    )

    batch = staged(spark, CUSTOMERS, [customer(1, 10, "Ada")])
    writes = gold.apply(BATCHES.writes("dim_customer", batch, gold.read))

    (version,) = gold.read("dim_customer").collect()
    assert rows(writes, "fact_order_history", "order_id", "customer_key") == [
        (7, version.customer_key)
    ]
    assert rows(writes, "top_customers_by_pending_orders", "customer_id", "customer_name") == [
        (1, "Ada")
    ]
    assert rows_of(gold.read("fact_order_history"), "order_id", "_is_current") == [(7, True)]


def test_a_replayed_batch_writes_no_versions(spark: SparkSession) -> None:
    gold = MemoryGold(spark)
    batch = staged(spark, ORDERS, [order(7, 10)])
    gold.apply(BATCHES.writes("fact_order_history", batch, gold.read))

    writes = BATCHES.writes("fact_order_history", batch, gold.read)

    assert rows(writes, "fact_order_history", "order_id") == []
    assert rows(writes, "current_orders", "order_id") == []


def test_merged_rows_replace_the_stored_rows_with_the_same_keys(spark: SparkSession) -> None:
    stored = spark.createDataFrame([(1, "a"), (2, "b")], "id long, name string")
    upserts = spark.createDataFrame([(2, "c"), (3, "d")], "id long, name string")

    assert rows_of(merged(stored, upserts, ("id",)), "id", "name") == [
        (1, "a"),
        (2, "c"),
        (3, "d"),
    ]


def test_a_recount_replaces_its_keys_and_deletes_the_removed_ones_nulls_matching(
    spark: SparkSession,
) -> None:
    stored = spark.createDataFrame([(None, 1), (1, 2), (2, 3)], "id long, total long")
    recount = spark.createDataFrame(
        [(None, 5, False), (1, None, True)], "id long, total long, _is_removed boolean"
    )

    assert rows_of(recounted(stored, recount, ("id",)), "id", "total") == [(None, 5), (2, 3)]


def test_resolved_history_rows_keep_their_other_columns(spark: SparkSession) -> None:
    history = spark.createDataFrame(
        [(7, 10, None, "x"), (7, 20, None, "y")],
        "order_id long, _commit_sequence long, customer_key long, other string",
    )
    resolved = spark.createDataFrame(
        [(7, 20, 99)], "order_id long, _commit_sequence long, customer_key long"
    )

    assert rows_of(with_resolved(history, resolved, ORDERS), *history.columns) == [(7, 20, 99, "y")]
