import pytest

from tests.infra.stack import Stack

FIXTURE_APP = "tests/infra/apps/landing_fixture.py"
EXAMPLES = "tools/spark-sql/landing_examples.sql"
FIXTURE_ROOT = "s3a://lake/test-fixtures/landing"
ORDERS = "finance_db.operations.orders"


@pytest.fixture(scope="module")
def results(stack: Stack) -> dict[str, list[list[str]]]:
    """Run the example queries against fixture records and group their rows by example label."""
    returncode, output = stack.run_app(FIXTURE_APP, FIXTURE_ROOT)
    assert returncode == 0, output

    run = stack.run_sql_file(EXAMPLES, FIXTURE_ROOT)

    assert run.returncode == 0, run.stdout + run.stderr
    grouped: dict[str, list[list[str]]] = {}
    for line in run.stdout.splitlines():
        label, _, rest = line.partition("\t")
        if rest:
            grouped.setdefault(label, []).append(rest.split("\t"))
    return grouped


def test_latest_records_come_first(results: dict[str, list[list[str]]]) -> None:
    offsets = [row[2] for row in results["latest"] if row[0] == ORDERS]

    assert offsets == ["4", "3", "2", "1", "0"]


def test_every_change_of_one_key_is_returned_in_order(
    results: dict[str, list[list[str]]],
) -> None:
    assert [(row[2], row[4]) for row in results["by key"]] == [("0", "create"), ("2", "update")]
    assert '"SHIPPED"' in results["by key"][1][5]


def test_offset_range_is_inclusive(results: dict[str, list[list[str]]]) -> None:
    assert [row[2] for row in results["offset range"]] == ["1", "2", "3"]


def test_recent_records_include_every_topic(results: dict[str, list[list[str]]]) -> None:
    assert {row[0] for row in results["last hour"]} == {
        f"finance_db.operations.{table}"
        for table in ("customers", "products", "orders", "order_items")
    }


def test_counts_by_operation_decode_every_topic(results: dict[str, list[list[str]]]) -> None:
    counts = {(row[0], row[1]): row[2] for row in results["counts"]}

    assert counts[(ORDERS, "create")] == "2"
    assert counts[(ORDERS, "update")] == "1"
    assert counts[(ORDERS, "delete")] == "1"
    assert counts[(ORDERS, "tombstone")] == "1"
    for table in ("customers", "products", "order_items"):
        assert counts[(f"finance_db.operations.{table}", "snapshot")] == "1"


def test_tombstones_are_listed(results: dict[str, list[list[str]]]) -> None:
    assert results["tombstones"] == [[ORDERS, "0", "4", '{"order_id": 2}']]
