from collections.abc import Iterator

import pytest

from tests.infra.gold_fixtures import order_change, rolled_back, rows
from tests.infra.gold_retention_setup import (
    COMMITTED_ORDER,
    DEFAULT_ROWS,
    RETAIN,
    first_service_loop,
    in_month,
    make_gold_retention,
    old_item,
    old_open_order,
    partitions,
    remove_committed_order,
    suffix,
)
from tests.infra.stack import Stack

OUTSIDE = -37
FIRST_KEPT = -36


def test_retention_drops_only_the_months_that_end_before_the_window(stack: Stack) -> None:
    result = rows(
        stack,
        rolled_back(
            order_change(1, 10, in_month(OUTSIDE)),
            order_change(2, 10, in_month(FIRST_KEPT)),
            order_change(3, 10, in_month(0)),
            old_item(1, OUTSIDE),
            RETAIN,
            "SELECT order_id FROM gold_internal.fact_order_history ORDER BY 1;",
            "SELECT count(*) FROM gold_internal.fact_order_item_history;",
        ),
    )
    month = suffix(stack, OUTSIDE)

    assert result == [
        [f"gold_internal.fact_order_history_{month}"],
        [f"gold_internal.fact_order_item_history_{month}"],
        ["2"],
        ["3"],
        ["0"],
    ]


def test_retention_partitions_the_months_the_default_partition_holds(stack: Stack) -> None:
    result = rows(
        stack,
        rolled_back(
            order_change(1, 10, in_month(-12)),
            order_change(2, 10, in_month(FIRST_KEPT)),
            DEFAULT_ROWS,
            RETAIN,
            "SELECT tableoid::regclass, to_char(_changed_at, 'YYYY_MM')"
            " FROM gold_internal.fact_order_history ORDER BY order_id;",
            DEFAULT_ROWS,
        ),
    )
    before, *placed, after = result

    assert before == ["2"]
    assert [table for table, _ in placed] == [
        f"gold_internal.fact_order_history_{month}" for _, month in placed
    ]
    assert after == ["0"]


def test_open_orders_older_than_the_window_stay_in_current_state(stack: Stack) -> None:
    result = rows(
        stack,
        rolled_back(
            *old_open_order(1, -40),
            RETAIN,
            "SELECT count(*) FROM gold_internal.fact_order_history WHERE order_id = 1;",
            "SELECT order_id FROM gold.current_orders WHERE order_id = 1;",
        ),
    )

    assert result[-2:] == [["0"], ["1"]]


def test_applying_the_ddl_creates_no_monthly_partitions(stack: Stack) -> None:
    result = rows(stack, partitions("fact_order_history") + partitions("fact_order_item_history"))

    assert result == [["fact_order_history_default"], ["fact_order_item_history_default"]]


def test_retention_creates_this_month_and_the_next_two_by_default(stack: Stack) -> None:
    result = rows(stack, rolled_back(RETAIN, partitions("fact_order_history")))
    months = [f"fact_order_history_{suffix(stack, offset)}" for offset in range(3)]

    assert result == [[name] for name in [*months, "fact_order_history_default"]]


def test_the_first_retention_moves_this_months_rows_out_of_the_default_partition(
    stack: Stack,
) -> None:
    result = rows(
        stack,
        rolled_back(
            order_change(1, 10, in_month(0)),
            RETAIN,
            "SELECT tableoid::regclass FROM gold_internal.fact_order_history;",
        ),
    )

    assert result == [[f"gold_internal.fact_order_history_{suffix(stack, 0)}"]]


def test_retention_creates_the_partitions_of_the_months_ahead(stack: Stack) -> None:
    result = rows(
        stack,
        rolled_back(
            "SELECT count(*) FROM gold_internal.apply_history_retention(36, 4, current_date);",
            partitions("fact_order_item_history"),
        ),
    )
    ahead = [f"fact_order_item_history_{suffix(stack, offset)}" for offset in range(5)]

    assert result == [["0"], *([name] for name in [*ahead, "fact_order_item_history_default"])]


def test_retention_applied_again_drops_nothing_more(stack: Stack) -> None:
    result = rows(
        stack,
        rolled_back(order_change(1, 10, in_month(OUTSIDE)), RETAIN, "SELECT 'again';", RETAIN),
    )

    assert result == [[f"gold_internal.fact_order_history_{suffix(stack, OUTSIDE)}"], ["again"]]


@pytest.fixture
def committed_old_order(stack: Stack) -> Iterator[None]:
    stack.sql("analytics", order_change(COMMITTED_ORDER, 10, in_month(OUTSIDE)))
    try:
        yield
    finally:
        remove_committed_order(stack)


@pytest.mark.usefixtures("committed_old_order")
def test_make_gold_retention_drops_the_months_outside_the_window(stack: Stack) -> None:
    run = make_gold_retention(stack)
    left = stack.sql(
        "analytics",
        "SELECT count(*) FROM gold_internal.fact_order_history"
        f" WHERE order_id = {COMMITTED_ORDER};",
    )

    assert run.returncode == 0, run.stderr
    assert "dropped 1 partition(s)" in run.stdout
    assert f"gold_internal.fact_order_history_{suffix(stack, OUTSIDE)}" in run.stdout
    assert left == "0"


@pytest.mark.usefixtures("committed_old_order")
def test_the_maintenance_service_applies_retention_on_its_first_run(stack: Stack) -> None:
    log = first_service_loop(stack)

    assert "dropped 1 partition(s)" in log
    assert f"gold_internal.fact_order_history_{suffix(stack, OUTSIDE)}" in log
