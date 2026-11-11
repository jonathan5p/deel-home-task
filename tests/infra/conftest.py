import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from xdist.remote import Producer
from xdist.scheduler import LoadScopeScheduling

from tests.infra.bronze_replay_setup import ReplayRuns, run_replay_scenario
from tests.infra.bronze_setup import BronzeRuns, run_bronze_scenario
from tests.infra.gold_dimensions_setup import GoldRuns, remove_fixture_rows, run_gold_scenario
from tests.infra.gold_facts_setup import FactRuns, remove_fact_rows, run_fact_scenario
from tests.infra.gold_lake_setup import LakeComparison, run_lake_scenario
from tests.infra.lake_catalog_setup import LakeCatalog, run_catalog_scenario
from tests.infra.landing_avro import landing_avro
from tests.infra.silver_setup import SilverRuns, run_silver_scenario
from tests.infra.stack import SHARED_GROUP, Stack, misplaced_scenario_users, xdist_scope

LANDING_FIXTURE_ROOT = "test-fixtures/landing"
SESSION_SCENARIOS = ("landing_fixture", "bronze_runs", "silver_runs")


class InfraScheduling(LoadScopeScheduling):
    """`--dist loadscope` that sends the modules of a group in `XDIST_GROUPS` to one worker.
    pytest-xdist's own `loadgroup` cannot rename the test ids under pytest 9, so its groups have
    no effect."""

    def _split_scope(self, nodeid: str) -> str:
        return xdist_scope(nodeid)


def pytest_xdist_make_scheduler(config: pytest.Config, log: Producer) -> LoadScopeScheduling | None:
    return InfraScheduling(config, log) if config.getvalue("dist") == "loadscope" else None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    fixtures: dict[str, set[str]] = {}
    for item in items:
        if isinstance(item, pytest.Function):
            fixtures.setdefault(item.nodeid.split("::")[0], set()).update(item.fixturenames)
    if misplaced := misplaced_scenario_users(fixtures, SESSION_SCENARIOS):
        raise pytest.UsageError(
            f"{', '.join(misplaced)} use a session scenario; add them to XDIST_GROUPS as"
            f" {SHARED_GROUP!r}, or a parallel run repeats the scenario."
        )


@pytest.fixture(scope="session")
def stack() -> Iterator[Stack]:
    platform = Stack.isolated(os.environ)
    platform.down()
    platform.up()
    try:
        yield platform
    finally:
        platform.down()


def change_event(
    op: str, before: dict[str, object] | None, after: dict[str, object] | None
) -> bytes:
    return json.dumps({"before": before, "after": after, "op": op, "ts_ms": 0}).encode()


def landing_records(now: datetime) -> dict[str, list[dict[str, object]]]:
    """Neutral landing records per topic: a create, an update, a delete, a tombstone, a snapshot."""

    def record(topic: str, offset: int, key: int, value: bytes | None) -> dict[str, object]:
        return {
            "key": json.dumps({"id": key}).encode(),
            "value": value,
            "topic": topic,
            "partition": 0,
            "offset": offset,
            "timestamp": now - timedelta(minutes=10 - offset),
            "timestampType": 0,
        }

    first = {"id": 1, "state": "a"}
    first_changed = {"id": 1, "state": "b"}
    second = {"id": 2, "state": "a"}
    third = {"id": 3, "state": "a"}
    return {
        "topic-a": [
            record("topic-a", 0, 1, change_event("c", None, first)),
            record("topic-a", 1, 2, change_event("c", None, second)),
            record("topic-a", 2, 1, change_event("u", first, first_changed)),
            record("topic-a", 3, 2, change_event("d", second, None)),
            record("topic-a", 4, 2, None),
            record("topic-a", 5, 3, change_event("r", None, third)),
        ],
        "topic-b": [record("topic-b", 0, 1, change_event("c", None, first))],
    }


@pytest.fixture(scope="session")
def landing_fixture(stack: Stack) -> str:
    """Upload the neutral landing records as Avro files and return their landing root path."""
    for topic, records in landing_records(datetime.now(UTC)).items():
        stack.put_object(f"{LANDING_FIXTURE_ROOT}/{topic}/part-00000.avro", landing_avro(records))
    return LANDING_FIXTURE_ROOT


@pytest.fixture(scope="session")
def bronze_runs(stack: Stack) -> BronzeRuns:
    return run_bronze_scenario(stack)


@pytest.fixture(scope="session")
def silver_runs(stack: Stack, bronze_runs: BronzeRuns) -> SilverRuns:
    """Silver runs on the bronze tables the bronze scenario leaves, which it then extends."""
    return run_silver_scenario(stack, bronze_runs.idle)


@pytest.fixture(scope="module")
def gold_runs(stack: Stack) -> Iterator[GoldRuns]:
    """Run the gold job over the fixture silver batches, then remove the rows it wrote, which
    the gold model tests must not see."""
    try:
        yield run_gold_scenario(stack)
    finally:
        remove_fixture_rows(stack)


@pytest.fixture(scope="module")
def fact_runs(stack: Stack) -> Iterator[FactRuns]:
    """Run the gold job over the fixture silver changes, then remove the rows it wrote, which
    the gold model tests must not see."""
    try:
        yield run_fact_scenario(stack)
    finally:
        remove_fact_rows(stack)


@pytest.fixture(scope="module")
def replay_runs(stack: Stack) -> ReplayRuns:
    return run_replay_scenario(stack)


@pytest.fixture(scope="module")
def lake_comparison(stack: Stack) -> Iterator[LakeComparison]:
    try:
        yield run_lake_scenario(stack)
    finally:
        remove_fact_rows(stack)


@pytest.fixture(scope="module")
def lake_catalog(stack: Stack, silver_runs: SilverRuns) -> LakeCatalog:
    """The REST catalog over the bronze and silver tables and views the silver scenario leaves."""
    return run_catalog_scenario(stack)
