import subprocess
from dataclasses import dataclass

from tests.infra.gold_facts_setup import (
    BATCHES,
    CUSTOMERS,
    PRODUCTS,
    FactState,
    Table,
    fact_state,
    remove_fact_rows,
)
from tests.infra.gold_fixtures import rows
from tests.infra.gold_silver_setup import Batches, SilverFixture, changed_at, keys
from tests.infra.stack import REPO_ROOT, Stack

CUTOFF_SEQUENCE = 250
CUTOFF = changed_at(CUTOFF_SEQUENCE)
STREAMED_BATCHES = ("batch-1", "batch-2", "batch-late")

CUSTOMER_VERSIONS = """
SELECT customer_id, _commit_sequence, customer_name, _is_folded, _is_deleted, _valid_from,
    _valid_to, _is_current
FROM gold_internal.dim_customer_versions
WHERE customer_id IN ({customers})
ORDER BY 1, 2;
"""
PRODUCT_VERSIONS = """
SELECT product_id, _commit_sequence, unit_price, _is_folded, _is_deleted, _valid_from,
    _valid_to, _is_current
FROM gold_internal.dim_product_versions
WHERE product_id IN ({products})
ORDER BY 1, 2;
"""


def committed_by(batches: Batches, sequence: int) -> Batches:
    """The batches with only the changes at or before `sequence`, in the same arrival order."""
    return {
        name: {
            entity: [row for row in changes if int(str(row["_commit_sequence"])) <= sequence]
            for entity, changes in batch.items()
        }
        for name, batch in batches.items()
    }


EVERY_CHANGE = SilverFixture("test-fixtures/gold-rebuild/all", "gold_rebuild_all", BATCHES)
UP_TO_CUTOFF = SilverFixture(
    "test-fixtures/gold-rebuild/cutoff",
    "gold_rebuild_cutoff",
    committed_by(BATCHES, CUTOFF_SEQUENCE),
)


@dataclass(frozen=True, slots=True)
class GoldState:
    facts: FactState
    customers: Table
    products: Table


@dataclass(frozen=True, slots=True)
class RebuildRuns:
    streamed: GoldState
    streamed_up_to_cutoff: GoldState
    rebuilt: GoldState
    rebuilt_up_to_cutoff: GoldState
    rebuild_output: str


def gold_state(stack: Stack) -> GoldState:
    fixture = {"customers": keys(CUSTOMERS), "products": keys(PRODUCTS)}
    return GoldState(
        facts=fact_state(stack),
        customers=rows(stack, CUSTOMER_VERSIONS.format(**fixture)),
        products=rows(stack, PRODUCT_VERSIONS.format(**fixture)),
    )


def taken(stack: Stack) -> GoldState:
    """The scenario's gold rows, which are then removed for the next run."""
    state = gold_state(stack)
    remove_fact_rows(stack)
    return state


def run_rebuild_scenario(stack: Stack) -> RebuildRuns:
    remove_fact_rows(stack)
    EVERY_CHANGE.run_gold(stack, *STREAMED_BATCHES)
    streamed = taken(stack)
    UP_TO_CUTOFF.run_gold(stack, *STREAMED_BATCHES)
    streamed_up_to_cutoff = taken(stack)
    output = EVERY_CHANGE.rebuild_gold(stack)
    rebuilt = taken(stack)
    EVERY_CHANGE.rebuild_gold(stack, "--to-time", CUTOFF.isoformat())
    rebuilt_up_to_cutoff = taken(stack)
    return RebuildRuns(
        streamed=streamed,
        streamed_up_to_cutoff=streamed_up_to_cutoff,
        rebuilt=rebuilt,
        rebuilt_up_to_cutoff=rebuilt_up_to_cutoff,
        rebuild_output=output,
    )


def make_gold_rebuild(stack: Stack, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "gold-rebuild", *args],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        check=False,
    )
