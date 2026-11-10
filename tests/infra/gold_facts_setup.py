from dataclasses import dataclass
from datetime import timedelta

from tests.infra.gold_fixtures import GOLD_QUERIES, rows
from tests.infra.gold_silver_setup import Batches, Json, SilverFixture, changed_at, keys, version
from tests.infra.stack import Stack

BASIC_CUSTOMER = 9401
LATE_CUSTOMER = 9402
UNFOLDED_CUSTOMER = 9403
CUSTOMERS = (BASIC_CUSTOMER, LATE_CUSTOMER, UNFOLDED_CUSTOMER)
PRICE_CHANGE = 9301
UNFOLDED_PRODUCT = 9302
EARLY_ITEM_PRODUCT = 9303
SNAPSHOTTED_LATER = 9304
PRODUCTS = (PRICE_CHANGE, UNFOLDED_PRODUCT, EARLY_ITEM_PRODUCT, SNAPSHOTTED_LATER)

CHANGE_SEQUENCE = 9501
OUT_OF_ORDER = 9502
DELETED = 9503
RESNAPSHOT = 9504
ITEMS_FIRST = 9505
CUSTOMER_LATER = 9506
UNFOLDED_ORDER = 9507
REOPENED = 9508
ORDERS = (
    CHANGE_SEQUENCE,
    OUT_OF_ORDER,
    DELETED,
    RESNAPSHOT,
    ITEMS_FIRST,
    CUSTOMER_LATER,
    UNFOLDED_ORDER,
    REOPENED,
)

BEFORE_PRICE_CHANGE = 9601
AFTER_PRICE_CHANGE = 9602
BEFORE_ITS_ORDER = 9603
IN_UNFOLDED_VERSION = 9604
IN_SPLIT_RANGE = 9605
BEFORE_ITS_PRODUCT = 9606
ITEMS = (
    BEFORE_PRICE_CHANGE,
    AFTER_PRICE_CHANGE,
    BEFORE_ITS_ORDER,
    IN_UNFOLDED_VERSION,
    IN_SPLIT_RANGE,
    BEFORE_ITS_PRODUCT,
)

type Table = list[list[str]]


def customer(customer_id: int, sequence: int, name: str, op: str = "update") -> Json:
    content: Json = {"customer_name": name, "is_active": True, "customer_address": "Main Street 1"}
    return {"customer_id": customer_id, **version(content, sequence, op)}


def product(product_id: int, sequence: int, price: str, op: str = "update") -> Json:
    content: Json = {
        "product_name": f"Product {product_id}",
        "barcode": str(product_id),
        "unity_price": float(price),
        "is_active": True,
    }
    return {"product_id": product_id, **version(content, sequence, op)}


def order(
    order_id: int,
    sequence: int,
    status: str | None,
    op: str = "update",
    *,
    customer_id: int = BASIC_CUSTOMER,
    delivery_date: str | None = "2026-11-05",
    deleted: bool = False,
) -> Json:
    content: Json = {
        "order_date": "2026-11-01",
        "delivery_date": delivery_date,
        "customer_id": customer_id,
        "status": status,
    }
    return {"order_id": order_id, **version(content, sequence, op, deleted=deleted)}


def item(
    order_item_id: int,
    sequence: int,
    order_id: int,
    product_id: int,
    quantity: int,
    *,
    earlier_ms: int = 0,
) -> Json:
    """One item change; `earlier_ms` moves its commit time that many milliseconds back."""
    content: Json = {"order_id": order_id, "product_id": product_id, "quantity": quantity}
    row = {"order_item_id": order_item_id, **version(content, sequence, "create")}
    moved = changed_at(sequence) - timedelta(milliseconds=earlier_ms)
    return row | {"_changed_at": moved.isoformat(timespec="milliseconds")}


BATCHES: Batches = {
    "batch-1": {
        "customers": [
            customer(BASIC_CUSTOMER, 5, "Cleo", "snapshot"),
            customer(UNFOLDED_CUSTOMER, 500, "Bea", "create"),
            customer(UNFOLDED_CUSTOMER, 540, "Bea", "snapshot"),
        ],
        "products": [
            product(PRICE_CHANGE, 10, "1.00", "snapshot"),
            product(PRICE_CHANGE, 30, "2.00"),
            product(UNFOLDED_PRODUCT, 200, "3.00", "create"),
            product(UNFOLDED_PRODUCT, 240, "3.00", "snapshot"),
            product(EARLY_ITEM_PRODUCT, 380, "5.00", "create"),
            product(SNAPSHOTTED_LATER, 600, "7.00", "snapshot"),
        ],
        "orders": [
            order(CHANGE_SEQUENCE, 20, "PENDING", "create"),
            order(CHANGE_SEQUENCE, 40, "PROCESSING"),
            order(OUT_OF_ORDER, 100, "PROCESSING"),
            order(DELETED, 110, None, "create"),
            order(DELETED, 120, None, "delete", deleted=True),
            order(RESNAPSHOT, 130, "ON HOLD", "create", delivery_date=None),
            order(RESNAPSHOT, 140, "ON HOLD", "snapshot", delivery_date=None),
            order(CUSTOMER_LATER, 300, "PENDING", "create", customer_id=LATE_CUSTOMER),
            order(UNFOLDED_ORDER, 550, "PENDING", "create", customer_id=UNFOLDED_CUSTOMER),
            order(REOPENED, 700, "PENDING", "create"),
        ],
        "order_items": [
            item(BEFORE_PRICE_CHANGE, 21, CHANGE_SEQUENCE, PRICE_CHANGE, 3),
            item(AFTER_PRICE_CHANGE, 35, CHANGE_SEQUENCE, PRICE_CHANGE, 2),
            item(BEFORE_ITS_ORDER, 400, ITEMS_FIRST, EARLY_ITEM_PRODUCT, 4),
            item(IN_UNFOLDED_VERSION, 250, UNFOLDED_ORDER, UNFOLDED_PRODUCT, 2),
            item(IN_SPLIT_RANGE, 230, UNFOLDED_ORDER, UNFOLDED_PRODUCT, 1),
            item(BEFORE_ITS_PRODUCT, 600, UNFOLDED_ORDER, SNAPSHOTTED_LATER, 2, earlier_ms=5),
        ],
    },
    "batch-2": {
        "customers": [
            customer(LATE_CUSTOMER, 290, "Lena", "create"),
            customer(UNFOLDED_CUSTOMER, 520, "Bea Smith"),
        ],
        "products": [product(UNFOLDED_PRODUCT, 220, "4.00")],
        "orders": [
            order(CHANGE_SEQUENCE, 60, "COMPLETED"),
            order(OUT_OF_ORDER, 90, "PENDING", "create"),
            order(ITEMS_FIRST, 395, "PENDING", "create"),
            order(REOPENED, 720, "COMPLETED"),
        ],
    },
    "batch-late": {
        "orders": [
            order(DELETED, 115, "PROCESSING"),
            order(CHANGE_SEQUENCE, 50, "PENDING"),
            order(REOPENED, 740, "REPROCESSING"),
        ]
    },
}
SILVER = SilverFixture("test-fixtures/gold-facts", "gold_facts_fixture", BATCHES)

# Surrogate keys are generated, so a looked-up version is shown by its _commit_sequence.
ORDER_HISTORY = """
SELECT f.order_id, f._commit_sequence, f._changed_at, f._change_op, f.status_key,
    c._commit_sequence, f.order_date_key, f.delivery_date_key, f._is_deleted
FROM gold_internal.fact_order_history AS f
LEFT JOIN gold_internal.dim_customer AS c ON c.customer_key = f.customer_key
WHERE f.order_id IN ({orders})
ORDER BY 1, 2;
"""
PUBLIC_ORDER_HISTORY = """
SELECT f.order_id, f._changed_at, f._change_op, f.status_key, c._commit_sequence,
    f.order_date_key, f.delivery_date_key, f._valid_to, f._is_current
FROM gold.fact_order_history AS f
LEFT JOIN gold_internal.dim_customer AS c ON c.customer_key = f.customer_key
WHERE f.order_id IN ({orders})
ORDER BY 1, 2;
"""
CURRENT_ORDERS = """
SELECT order_id, status_key, customer_id, order_date_key, delivery_date_key, _commit_sequence,
    _changed_at, _is_deleted
FROM gold_internal.current_orders
WHERE order_id IN ({orders})
ORDER BY 1;
"""
OPEN_FLAGS = """
SELECT order_id, is_open FROM gold_internal.current_orders WHERE order_id IN ({orders}) ORDER BY 1;
"""
OPEN_ORDERS = """
SELECT order_id FROM gold.open_orders WHERE order_id IN ({orders}) ORDER BY 1;
"""
PUBLIC_CURRENT_ORDERS = """
SELECT order_id FROM gold.current_orders WHERE order_id IN ({orders}) ORDER BY 1;
"""
ITEM_HISTORY = """
SELECT i.order_item_id, i._commit_sequence, p._commit_sequence, i.unit_price, i.line_amount
FROM gold_internal.fact_order_item_history AS i
LEFT JOIN gold_internal.dim_product AS p ON p.product_key = i.product_key
WHERE i.order_item_id IN ({items})
ORDER BY 1, 2;
"""
CURRENT_ITEMS = """
SELECT order_item_id, order_id, product_id, quantity, _commit_sequence, _is_deleted
FROM gold_internal.current_order_items
WHERE order_item_id IN ({items})
ORDER BY 1;
"""


@dataclass(frozen=True, slots=True)
class FactState:
    """The fixture rows of the facts and current-state tables, and what consumers see."""

    order_history: Table
    public_order_history: Table
    current_orders: Table
    open_flags: Table
    open_orders: Table
    public_current_orders: Table
    item_history: Table
    current_items: Table
    pending_items: Table


@dataclass(frozen=True, slots=True)
class FactRuns:
    first: FactState
    second: FactState
    replayed: FactState
    late: FactState


def fact_state(stack: Stack) -> FactState:
    fixture = {"orders": keys(ORDERS), "items": keys(ITEMS)}
    pending = rows(stack, (GOLD_QUERIES / "03_pending_items_by_product.sql").read_text())
    return FactState(
        order_history=rows(stack, ORDER_HISTORY.format(**fixture)),
        public_order_history=rows(stack, PUBLIC_ORDER_HISTORY.format(**fixture)),
        current_orders=rows(stack, CURRENT_ORDERS.format(**fixture)),
        open_flags=rows(stack, OPEN_FLAGS.format(**fixture)),
        open_orders=rows(stack, OPEN_ORDERS.format(**fixture)),
        public_current_orders=rows(stack, PUBLIC_CURRENT_ORDERS.format(**fixture)),
        item_history=rows(stack, ITEM_HISTORY.format(**fixture)),
        current_items=rows(stack, CURRENT_ITEMS.format(**fixture)),
        pending_items=[row for row in pending if int(row[0]) in PRODUCTS],
    )


def run_gold(stack: Stack, *batches: str, checkpoints: str = "main") -> FactState:
    SILVER.run_gold(stack, *batches, checkpoints=checkpoints)
    return fact_state(stack)


def run_fact_scenario(stack: Stack) -> FactRuns:
    """Two batches, a replay of everything from a fresh checkpoint, then a late change."""
    first = run_gold(stack, "batch-1")
    second = run_gold(stack, "batch-2")
    replayed = run_gold(stack, checkpoints="replay")
    late = run_gold(stack, "batch-late")
    return FactRuns(first=first, second=second, replayed=replayed, late=late)


def remove_fact_rows(stack: Stack) -> None:
    """Remove the scenario's rows, the facts first because they reference the dimensions."""
    rows(
        stack,
        f"DELETE FROM gold_internal.fact_order_item_history WHERE order_item_id IN ({keys(ITEMS)});"
        f" DELETE FROM gold_internal.current_order_items WHERE order_item_id IN ({keys(ITEMS)});"
        f" DELETE FROM gold_internal.fact_order_history WHERE order_id IN ({keys(ORDERS)});"
        f" DELETE FROM gold_internal.current_orders WHERE order_id IN ({keys(ORDERS)});"
        f" DELETE FROM gold_internal.dim_product WHERE product_id IN ({keys(PRODUCTS)});"
        f" DELETE FROM gold_internal.dim_customer WHERE customer_id IN ({keys(CUSTOMERS)});"
        " TRUNCATE gold_internal.stage_fact_order_history,"
        " gold_internal.stage_fact_order_item_history,"
        " gold_internal.stage_dim_product, gold_internal.stage_dim_customer;",
    )


def by_key(table: Table, key: int) -> Table:
    return [row for row in table if row[0] == str(key)]
