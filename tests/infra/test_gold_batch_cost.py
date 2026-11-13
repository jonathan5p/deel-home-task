import json
from collections import defaultdict
from statistics import median

import pytest

from tests.infra.gold_batch_cost_setup import UPDATES, BatchCost, run_batches
from tests.infra.stack import Stack

LOADS = {"dim_customer", "dim_product", "fact_order_history", "fact_order_item_history"}
MAX_JOBS = 30


@pytest.fixture(scope="module")
def costs(stack: Stack) -> list[BatchCost]:
    return run_batches(stack)


def test_every_load_logs_one_line_per_batch(costs: list[BatchCost]) -> None:
    for run in ("snapshot", *UPDATES):
        assert {cost.load for cost in costs if cost.run == run} == LOADS, run


def test_a_batch_runs_a_few_spark_jobs(costs: list[BatchCost]) -> None:
    by_load: dict[str, list[BatchCost]] = defaultdict(list)
    for cost in costs:
        by_load[cost.load].append(cost)
    summary = {
        load: {
            "jobs": [cost.jobs for cost in batches],
            "seconds": [cost.seconds for cost in batches],
            "median_update_seconds": median(c.seconds for c in batches if c.run != "snapshot"),
            "steps": batches[-1].steps,
        }
        for load, batches in sorted(by_load.items())
    }
    print(json.dumps(summary, indent=1))

    assert all(0 < cost.jobs <= MAX_JOBS for cost in costs if cost.run != "snapshot"), summary


def test_a_batch_writes_only_its_own_tables(costs: list[BatchCost]) -> None:
    own = {
        "dim_customer": {"dim_customer"},
        "dim_product": {"dim_product"},
        "fact_order_history": {"fact_order_history", "current_orders"},
        "fact_order_item_history": {"fact_order_item_history", "current_order_items"},
    }

    for cost in costs:
        assert set(cost.written) == own[cost.load]
