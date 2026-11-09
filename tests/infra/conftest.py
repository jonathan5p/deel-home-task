import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from tests.infra.landing_avro import landing_avro
from tests.infra.stack import Stack

LANDING_FIXTURE_ROOT = "test-fixtures/landing"


@pytest.fixture(scope="session")
def stack() -> Iterator[Stack]:
    platform = Stack.isolated()
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
