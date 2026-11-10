from dataclasses import dataclass

from tests.infra.gold_fixtures import rows
from tests.infra.gold_silver_setup import Batches, Json, SilverFixture, keys, version
from tests.infra.stack import Stack

PRICE_CHANGE = 9101
LATE_VERSION = 9102
RESNAPSHOT = 9103
DELETED = 9104
PRODUCTS = (PRICE_CHANGE, LATE_VERSION, RESNAPSHOT, DELETED)
AUDIT_ONLY = 9201
SINGLE_VERSION = 9202
CUSTOMERS = (AUDIT_ONLY, SINGLE_VERSION)

type Table = list[list[str]]


def product(
    product_id: int, sequence: int, price: str, op: str = "update", *, deleted: bool = False
) -> Json:
    content: Json = {
        "product_name": f"Product {product_id}",
        "barcode": str(product_id),
        "unity_price": float(price),
        "is_active": True,
    }
    return {"product_id": product_id, **version(content, sequence, op, deleted=deleted)}


def customer(
    customer_id: int, sequence: int, name: str, op: str = "update", *, updated_by: int = -1
) -> Json:
    content: Json = {"customer_name": name, "is_active": True, "customer_address": "Main Street 1"}
    row = version(content, sequence, op, updated_by=updated_by)
    return {"customer_id": customer_id, **row}


BATCHES: Batches = {
    "batch-1": {
        "products": [
            product(PRICE_CHANGE, 10, "1.00", "snapshot"),
            product(LATE_VERSION, 30, "5.00"),
            product(LATE_VERSION, 50, "7.00"),
            product(RESNAPSHOT, 60, "3.00"),
            product(RESNAPSHOT, 80, "3.00", "snapshot"),
            product(DELETED, 90, "9.00", "create"),
        ],
        "customers": [
            customer(AUDIT_ONLY, 10, "Ada", "snapshot"),
            customer(AUDIT_ONLY, 15, "Ada", updated_by=42),
        ],
    },
    "batch-2": {
        "products": [
            product(PRICE_CHANGE, 20, "2.00"),
            product(LATE_VERSION, 40, "6.00"),
            product(RESNAPSHOT, 70, "4.00"),
            product(DELETED, 100, "9.00", "delete", deleted=True),
        ],
        "customers": [customer(SINGLE_VERSION, 20, "Grace", "create")],
    },
    "batch-late": {"products": [product(DELETED, 95, "8.00")]},
}
SILVER = SilverFixture("test-fixtures/gold", "gold_fixture", BATCHES)


@dataclass(frozen=True, slots=True)
class GoldState:
    """Every stored version of the fixture keys, and what the public views show of them."""

    products: Table
    customers: Table
    public_products: Table
    public_customers: Table


@dataclass(frozen=True, slots=True)
class GoldRuns:
    first: GoldState
    second: GoldState
    replayed: GoldState
    late: GoldState


STORED = """
SELECT {key}, _commit_sequence, {surrogate}, {value}, _valid_from, coalesce(_valid_to::text, ''),
    _is_current, _is_folded, _is_deleted
FROM gold_internal.{table}_versions
WHERE {key} IN ({keys})
ORDER BY 1, 2;
"""
PUBLIC = """
SELECT {key}, {value}, _valid_from, coalesce(_valid_to::text, ''), _is_current
FROM gold.{table}
WHERE {key} IN ({keys})
ORDER BY 1, 3;
"""


def gold_state(stack: Stack) -> GoldState:
    product_query = {
        "key": "product_id",
        "surrogate": "product_key",
        "value": "unit_price",
        "table": "dim_product",
        "keys": keys(PRODUCTS),
    }
    customer_query = {
        "key": "customer_id",
        "surrogate": "customer_key",
        "value": "customer_name",
        "table": "dim_customer",
        "keys": keys(CUSTOMERS),
    }
    return GoldState(
        products=rows(stack, STORED.format(**product_query)),
        customers=rows(stack, STORED.format(**customer_query)),
        public_products=rows(stack, PUBLIC.format(**product_query)),
        public_customers=rows(stack, PUBLIC.format(**customer_query)),
    )


def run_gold(stack: Stack, *batches: str, checkpoints: str = "main") -> GoldState:
    """Append the batches to the fixture silver tables, run the gold job once, read gold."""
    SILVER.run_gold(stack, *batches, checkpoints=checkpoints)
    return gold_state(stack)


def run_gold_scenario(stack: Stack) -> GoldRuns:
    """Two batches, a replay of everything from a fresh checkpoint, then a late version."""
    first = run_gold(stack, "batch-1")
    second = run_gold(stack, "batch-2")
    replayed = run_gold(stack, checkpoints="replay")
    late = run_gold(stack, "batch-late")
    return GoldRuns(first=first, second=second, replayed=replayed, late=late)


def remove_fixture_rows(stack: Stack) -> None:
    rows(
        stack,
        f"DELETE FROM gold_internal.dim_product WHERE product_id IN ({keys(PRODUCTS)});"
        f" DELETE FROM gold_internal.dim_customer WHERE customer_id IN ({keys(CUSTOMERS)});"
        " TRUNCATE gold_internal.stage_dim_product, gold_internal.stage_dim_customer;",
    )


def by_key(table: Table, key: int) -> Table:
    return [row for row in table if row[0] == str(key)]


def versions(table: Table) -> Table:
    """Stored versions without their surrogate key, which the database generates."""
    return [[*row[:2], *row[3:]] for row in table]


def current_counts(state: GoldState) -> dict[str, int]:
    """The number of current versions of each fixture key."""
    counts: dict[str, int] = {}
    for row in (*state.products, *state.customers):
        counts[row[0]] = counts.get(row[0], 0) + (row[6] == "t")
    return counts
