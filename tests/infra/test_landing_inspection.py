import pytest

from tests.infra.stack import Stack

FIXTURE_APP = "tests/infra/apps/landing_fixture.py"
EXAMPLES = "tools/spark-sql/landing_examples.sql"
FIXTURE_ROOT = "s3a://lake/test-fixtures/landing"
VARIABLES = {
    "topic": "topic-a",
    "key_field": "id",
    "key_value": "1",
    "from_offset": "1",
    "to_offset": "3",
}


@pytest.fixture(scope="module")
def results(stack: Stack) -> dict[str, list[list[str]]]:
    """Run the example queries against fixture records and group their rows by example label."""
    returncode, output = stack.run_app(FIXTURE_APP, FIXTURE_ROOT)
    assert returncode == 0, output

    run = stack.run_sql_file(EXAMPLES, FIXTURE_ROOT, VARIABLES)

    assert run.returncode == 0, run.stdout + run.stderr
    grouped: dict[str, list[list[str]]] = {}
    for line in run.stdout.splitlines():
        label, _, rest = line.partition("\t")
        if rest:
            grouped.setdefault(label, []).append(rest.split("\t"))
    return grouped


def test_only_the_chosen_topic_is_inspected(results: dict[str, list[list[str]]]) -> None:
    topics = {row[0] for rows in results.values() for row in rows}

    assert topics == {"topic-a"}


def test_latest_records_come_first(results: dict[str, list[list[str]]]) -> None:
    assert [row[2] for row in results["latest"]] == ["5", "4", "3", "2", "1", "0"]


def test_every_change_of_one_key_is_returned_in_order(
    results: dict[str, list[list[str]]],
) -> None:
    assert [(row[2], row[4]) for row in results["by key"]] == [("0", "create"), ("2", "update")]
    assert '"state":"b"' in results["by key"][1][5]


def test_offset_range_is_inclusive(results: dict[str, list[list[str]]]) -> None:
    assert [row[2] for row in results["offset range"]] == ["1", "2", "3"]


def test_recent_records_are_returned(results: dict[str, list[list[str]]]) -> None:
    assert len(results["last hour"]) == 6


def test_counts_by_operation(results: dict[str, list[list[str]]]) -> None:
    counts = {row[1]: row[2] for row in results["counts"]}

    assert counts == {
        "create": "2",
        "update": "1",
        "delete": "1",
        "snapshot": "1",
        "tombstone": "1",
    }


def test_tombstones_are_listed(results: dict[str, list[list[str]]]) -> None:
    assert results["tombstones"] == [["topic-a", "0", "4", '{"id": 2}']]
