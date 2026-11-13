import json
import random
from collections.abc import Iterator
from dataclasses import dataclass

from tests.infra.gold_facts_setup import customer, item, order, product
from tests.infra.gold_silver_setup import Batches, Json, SilverFixture
from tests.infra.stack import Stack

BATCH_LINE = "GOLD BATCH "
CUSTOMERS = 300
PRODUCTS = 100
ORDERS = 3000
ITEMS_PER_ORDER = 3
UPDATES = ("update-1", "update-2", "update-3", "update-4")
STATUSES = ("PENDING", "PROCESSING", "REPROCESSING", "COMPLETED")
FIRST_KEY = 100_000
PAUSE_SECONDS = 45


def snapshot(sequence: Iterator[int]) -> dict[str, list[Json]]:
    customers = [
        customer(FIRST_KEY + c, next(sequence), f"Customer {c}", "snapshot")
        for c in range(CUSTOMERS)
    ]
    products = [
        product(FIRST_KEY + p, next(sequence), f"{1 + p % 50}.25", "snapshot")
        for p in range(PRODUCTS)
    ]
    orders = [
        order(
            FIRST_KEY + o,
            next(sequence),
            STATUSES[o % len(STATUSES)],
            "snapshot",
            customer_id=FIRST_KEY + o % CUSTOMERS,
        )
        for o in range(ORDERS)
    ]
    items = [
        item(
            FIRST_KEY + o * ITEMS_PER_ORDER + i,
            next(sequence),
            FIRST_KEY + o,
            FIRST_KEY + (o + i) % PRODUCTS,
            1 + i,
        )
        for o in range(ORDERS)
        for i in range(ITEMS_PER_ORDER)
    ]
    return {"customers": customers, "products": products, "orders": orders, "order_items": items}


def update(sequence: Iterator[int], picks: random.Random) -> dict[str, list[Json]]:
    """One source commit's worth of changes per entity, as the change stream delivers them."""
    customers = [
        customer(FIRST_KEY + picks.randrange(CUSTOMERS), next(sequence), f"Renamed {n}")
        for n in range(10)
    ]
    products = [
        product(FIRST_KEY + picks.randrange(PRODUCTS), next(sequence), f"{2 + n}.50")
        for n in range(5)
    ]
    orders = [
        order(
            FIRST_KEY + picks.randrange(ORDERS),
            next(sequence),
            picks.choice(STATUSES),
            customer_id=FIRST_KEY + picks.randrange(CUSTOMERS),
        )
        for _ in range(100)
    ]
    items = []
    for _ in range(200):
        key = picks.randrange(ORDERS * ITEMS_PER_ORDER)
        order_id = FIRST_KEY + key // ITEMS_PER_ORDER
        product_id = FIRST_KEY + picks.randrange(PRODUCTS)
        items.append(item(FIRST_KEY + key, next(sequence), order_id, product_id, 1 + key % 5))
    return {"customers": customers, "products": products, "orders": orders, "order_items": items}


def batches() -> Batches:
    sequence = iter(range(1, 10_000_000))
    picks = random.Random(7)
    generated = {"snapshot": snapshot(sequence)}
    for name in UPDATES:
        generated[name] = update(sequence, picks)
    return generated


SILVER = SilverFixture("test-fixtures/gold-batch-cost", "gold_batch_cost_fixture", batches())


@dataclass(frozen=True, slots=True)
class BatchCost:
    """The batch line a gold run logged for one load."""

    run: str
    load: str
    seconds: float
    jobs: int
    written: dict[str, int]
    steps: list[dict[str, object]]


def batch_costs(output: str) -> list[BatchCost]:
    """Every batch line in the job's output, the n-th batch of each load named after the n-th
    batch streamed."""
    costs = []
    seen: dict[str, int] = {}
    for line in output.splitlines():
        if BATCH_LINE not in line:
            continue
        logged = json.loads(line.split(BATCH_LINE, 1)[1])
        load = str(logged["load"])
        seen[load] = seen.get(load, -1) + 1
        costs.append(
            BatchCost(
                run=("snapshot", *UPDATES)[min(seen[load], len(UPDATES))],
                load=str(logged["load"]),
                seconds=float(logged["seconds"]),
                jobs=int(logged["jobs"]),
                written=dict(logged["written"]),
                steps=list(logged["steps"]),
            )
        )
    return costs


def run_batches(stack: Stack) -> list[BatchCost]:
    """Stream the snapshot, then each update, into one gold job, and collect every batch line;
    the snapshot's batch is the driver's first, the updates' run warm."""
    names = ("snapshot", *UPDATES)
    for name in names:
        SILVER.upload(stack, name)
    returncode, output = stack.run_app(
        "tests/infra/apps/gold_from_silver.py",
        SILVER.lake_key,
        "--stream",
        str(PAUSE_SECONDS),
        *names,
        env=SILVER.env("main"),
        timeout=900,
    )
    assert returncode == 0, output
    return batch_costs(output)
