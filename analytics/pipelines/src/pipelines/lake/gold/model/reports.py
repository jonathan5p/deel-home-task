from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from pipelines.lake.gold.model.versions import IS_DELETED, VersionColumn, all_of

IS_REMOVED = "_is_removed"
TOP_PLACES = 3

ORDER_ID = "order_id"
STATUS_KEY = "status_key"
STATUS_CODE = "status_code"
IS_OPEN = "is_open"
DELIVERY_DATE = "delivery_date"
CUSTOMER_ID = "customer_id"
CUSTOMER_NAME = "customer_name"
PRODUCT_ID = "product_id"
QUANTITY = "quantity"
OPEN_ORDERS = "open_orders"
PENDING_ORDERS = "pending_orders"
PENDING_ITEMS = "pending_items"
PENDING_QUANTITY = "pending_quantity"


def open_orders(orders: DataFrame) -> DataFrame:
    """The current-state orders that are open and not deleted."""
    return orders.where(F.col(IS_OPEN) & ~F.col(IS_DELETED))


def recount(
    touched: DataFrame, counted: DataFrame, keys: Sequence[str], totals: Sequence[Column]
) -> DataFrame:
    """The `totals` of the `counted` rows of every key in `touched`, recounted from the current
    state, with `_is_removed` set, and the totals null, where no row counts any more.

    A streaming groupBy over the change log cannot keep these totals: its state only adds what
    each change brings, so it cannot take back an order's previous delivery date, status or
    open state when a newer version replaces it, nor ignore a late version older than the
    stored one. Recounting the keys a batch touched, from both the old and the new values of
    its rows, keeps every total exact, and a replayed batch recounts the same numbers.

    The writer merges the result on `keys`, with null-safe equality because a key column can
    be null: when matched and `_is_removed`, delete the row; when matched otherwise, update the
    totals; when not matched and not `_is_removed`, insert the row.
    """
    touched_keys = touched.select(
        *(F.col(key).alias(f"__touched_{key}") for key in keys)
    ).distinct()
    matching = all_of([F.col(key).eqNullSafe(F.col(f"__touched_{key}")) for key in keys])
    sums = (
        counted.join(F.broadcast(touched_keys), matching, "left_semi")
        .groupBy(*keys)
        .agg(*totals, F.lit(True).alias("__found"))
    )
    total_names = sums.columns[len(keys) : -1]
    return touched_keys.join(sums, matching, "left").select(
        *(F.col(f"__touched_{key}").alias(key) for key in keys),
        *total_names,
        F.col("__found").isNull().alias(IS_REMOVED),
    )


def open_orders_by_delivery_date_and_status(
    changed_orders: DataFrame, orders: DataFrame, statuses: DataFrame
) -> DataFrame:
    """The open orders by delivery date and status code, recounted (`recount`) for every
    delivery date and status an open order of the batch had before or has after the merge.

    `changed_orders` holds the current-state rows of the batch's orders as they were before
    the merge and as `current_rows` leaves them; `orders` the current-state orders after the
    merge; `statuses` the seeded order statuses.
    """
    codes = F.broadcast(statuses.select(STATUS_KEY, STATUS_CODE))
    return recount(
        open_orders(changed_orders).join(codes, STATUS_KEY),
        open_orders(orders).join(codes, STATUS_KEY),
        (DELIVERY_DATE, STATUS_CODE),
        [F.count(F.lit(1)).alias(OPEN_ORDERS)],
    )


def pending_orders_by_customer(changed_orders: DataFrame, orders: DataFrame) -> DataFrame:
    """The open orders by customer, recounted (`recount`) for every customer with an open
    order of the batch before or after the merge; the inputs are those of
    `open_orders_by_delivery_date_and_status`."""
    return recount(
        open_orders(changed_orders),
        open_orders(orders),
        (CUSTOMER_ID,),
        [F.count(F.lit(1)).alias(PENDING_ORDERS)],
    )


def products_of_orders(changed_orders: DataFrame, items: DataFrame) -> DataFrame:
    """The products of the items, not deleted, of the batch's orders: an order opening or
    closing changes the pending items of each of them."""
    order_ids = changed_orders.select(ORDER_ID).distinct()
    live = items.where(~F.col(IS_DELETED))
    return live.join(order_ids, ORDER_ID, "left_semi").select(PRODUCT_ID)


def pending_items_by_product(
    changed_products: DataFrame, items: DataFrame, orders: DataFrame
) -> DataFrame:
    """The items, not deleted, of open orders by product, as a count and a total quantity,
    recounted (`recount`) for every product in `changed_products`.

    For an order items batch, `changed_products` holds the products of the batch's items before
    and after the merge; for an orders batch, `products_of_orders`. `items` and `orders` are
    the current-state rows after the merge.
    """
    open_ids = open_orders(orders).select(ORDER_ID)
    pending = items.where(~F.col(IS_DELETED)).join(open_ids, ORDER_ID, "left_semi")
    return recount(
        changed_products,
        pending,
        (PRODUCT_ID,),
        [
            F.count(F.lit(1)).alias(PENDING_ITEMS),
            F.sum(QUANTITY).cast(LongType()).alias(PENDING_QUANTITY),
        ],
    )


def _top(rows: DataFrame, total: str) -> DataFrame:
    """The rows ranked in the first `TOP_PLACES` places by total, as RANK() numbers them: rows
    that tie share a place and the next place is skipped, so every row tied for the last place
    comes back."""
    place = F.rank().over(Window.orderBy(F.col(total).desc()))
    return (
        rows.where(F.col(total) > 0)
        .withColumn("__place", place)
        .where(F.col("__place") <= TOP_PLACES)
        .drop("__place")
    )


def top_delivery_dates_by_open_orders(by_delivery_date_and_status: DataFrame) -> DataFrame:
    """The delivery dates with the most open orders, from the stored
    open_orders_by_delivery_date_and_status rows after the merge. The result is the whole
    table: the writer replaces its rows."""
    by_date = by_delivery_date_and_status.groupBy(DELIVERY_DATE).agg(
        F.sum(OPEN_ORDERS).cast(LongType()).alias(OPEN_ORDERS)
    )
    return _top(by_date, OPEN_ORDERS)


def top_customers_by_pending_orders(by_customer: DataFrame, customers: DataFrame) -> DataFrame:
    """The customers with the most pending orders, from the stored pending_orders_by_customer
    rows after the merge, with the name of the customer's current version in `customers` (the
    stored dim_customer rows), or null while there is none. The result is the whole table: the
    writer replaces its rows."""
    names = customers.where(F.col(VersionColumn.IS_CURRENT)).select(CUSTOMER_ID, CUSTOMER_NAME)
    top = _top(by_customer.select(CUSTOMER_ID, PENDING_ORDERS), PENDING_ORDERS)
    return top.join(names, CUSTOMER_ID, "left").select(CUSTOMER_ID, CUSTOMER_NAME, PENDING_ORDERS)
