from collections.abc import Iterator

import pytest

from tests.infra.gold_facts_setup import CUSTOMER_LATER, remove_fact_rows
from tests.infra.gold_rebuild_setup import RebuildRuns, make_gold_rebuild, run_rebuild_scenario
from tests.infra.stack import Stack


@pytest.fixture(scope="module")
def rebuild_runs(stack: Stack) -> Iterator[RebuildRuns]:
    try:
        yield run_rebuild_scenario(stack)
    finally:
        remove_fact_rows(stack)


def test_a_rebuild_from_silver_matches_the_streamed_gold(rebuild_runs: RebuildRuns) -> None:
    assert rebuild_runs.rebuilt == rebuild_runs.streamed


def test_a_rebuild_up_to_a_cutoff_matches_gold_streamed_from_the_changes_up_to_it(
    rebuild_runs: RebuildRuns,
) -> None:
    assert rebuild_runs.rebuilt_up_to_cutoff == rebuild_runs.streamed_up_to_cutoff


def test_the_cutoff_leaves_the_later_changes_out(rebuild_runs: RebuildRuns) -> None:
    def orders(rows: list[list[str]]) -> set[str]:
        return {row[0] for row in rows}

    assert str(CUSTOMER_LATER) in orders(rebuild_runs.rebuilt.facts.current_orders)
    assert str(CUSTOMER_LATER) not in orders(rebuild_runs.rebuilt_up_to_cutoff.facts.current_orders)
    assert rebuild_runs.rebuilt_up_to_cutoff.customers != rebuild_runs.rebuilt.customers


def test_the_rebuild_applies_retention_at_the_end(rebuild_runs: RebuildRuns) -> None:
    assert "retention dropped 0 partition(s)" in rebuild_runs.rebuild_output


def test_make_gold_rebuild_refuses_without_confirm(stack: Stack) -> None:
    refused = make_gold_rebuild(stack)

    assert refused.returncode != 0
    assert "CONFIRM=1" in refused.stderr
