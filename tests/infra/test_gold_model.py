import subprocess

import pytest

from tests.infra.gold_fixtures import (
    CURRENT_STATE,
    DELETED_ROWS,
    FRESHNESS,
    FRESHNESS_FIXTURE,
    GOLD_DDL,
    HISTORY_PARTITIONS,
    INTERNAL_TABLES,
    PUBLIC_PIPELINE_ONLY_COLUMNS,
    PUBLIC_VIEWS,
    STAGING_TABLES,
    TIES_AT_THE_TOP,
    UNDESCRIBED_COLUMNS,
    UNDESCRIBED_RELATIONS,
    UNDESCRIBED_SCHEMAS,
    answer,
    item_change,
    order_change,
    psql,
    rolled_back,
    rows,
    top_level_relations,
)
from tests.infra.stack import Stack

NEXT_MONTH = "date_trunc('month', localtimestamp) + interval '1 month 2 days'"
IN_TWO_MONTHS = "date_trunc('month', localtimestamp) + interval '2 months 2 days'"
IN_SIX_MONTHS = "date_trunc('month', localtimestamp) + interval '6 months 2 days'"
BEFORE_ANY_PARTITION = "timestamp '2001-01-01 00:00'"


@pytest.fixture(scope="module")
def reapplied(stack: Stack) -> list[subprocess.CompletedProcess[str]]:
    """Apply the gold DDL twice more on top of the init-script run on the empty volume."""
    ddl = GOLD_DDL.read_text()
    return [psql(stack, ddl), psql(stack, ddl)]


@pytest.fixture
def gold(stack: Stack, reapplied: list[subprocess.CompletedProcess[str]]) -> Stack:
    assert all(run.returncode == 0 for run in reapplied)
    return stack


def test_ddl_applies_again_to_an_existing_database(
    reapplied: list[subprocess.CompletedProcess[str]],
) -> None:
    for run in reapplied:
        assert run.returncode == 0, run.stderr
        assert "ERROR" not in run.stderr


def test_internal_schema_holds_the_model_tables(gold: Stack) -> None:
    relations = rows(gold, top_level_relations("gold_internal"))

    assert {name for name, _ in relations} == INTERNAL_TABLES | STAGING_TABLES
    assert {kind for _, kind in relations} <= {"r", "p"}


def test_staging_tables_are_unlogged(gold: Stack) -> None:
    persistence = rows(
        gold,
        "SELECT relname, relpersistence FROM pg_class"
        " WHERE relnamespace = 'gold_internal'::regnamespace AND relname LIKE 'stage_%'"
        " ORDER BY 1;",
    )

    assert persistence == [[name, "u"] for name in sorted(STAGING_TABLES)]


def test_public_schema_holds_only_views(gold: Stack) -> None:
    relations = rows(gold, top_level_relations("gold"))

    assert {name for name, _ in relations} == PUBLIC_VIEWS
    assert {kind for _, kind in relations} == {"v"}


def test_public_views_hide_the_pipeline_only_columns(gold: Stack) -> None:
    assert rows(gold, PUBLIC_PIPELINE_ONLY_COLUMNS) == []


def test_public_current_views_hide_deleted_rows(gold: Stack) -> None:
    assert rows(gold, rolled_back(CURRENT_STATE, DELETED_ROWS)) == [
        ["orders", "0", "9"],
        ["order_items", "0", "7"],
    ]


def test_every_schema_table_and_column_has_a_description(gold: Stack) -> None:
    assert rows(gold, UNDESCRIBED_SCHEMAS) == []
    assert rows(gold, UNDESCRIBED_RELATIONS) == []
    assert rows(gold, UNDESCRIBED_COLUMNS) == []


def test_order_statuses_are_seeded_with_their_open_flag(gold: Stack) -> None:
    statuses = rows(gold, "SELECT status_code, is_open FROM gold.dim_order_status;")

    assert dict(statuses) == {
        "PENDING": "t",
        "PROCESSING": "t",
        "REPROCESSING": "t",
        "COMPLETED": "f",
        "UNKNOWN": "t",
    }


def test_status_keys_are_stable(gold: Stack) -> None:
    keys = rows(gold, "SELECT status_key, status_code FROM gold.dim_order_status ORDER BY 1;")

    assert keys == [
        ["1", "PENDING"],
        ["2", "PROCESSING"],
        ["3", "REPROCESSING"],
        ["4", "COMPLETED"],
        ["5", "UNKNOWN"],
    ]


def test_dates_cover_2000_to_2099_once_each(gold: Stack) -> None:
    summary = rows(
        gold,
        "SELECT count(*), count(DISTINCT calendar_date), min(calendar_date), max(calendar_date)"
        " FROM gold.dim_date;",
    )

    assert summary == [["36525", "36525", "2000-01-01", "2099-12-31"]]


@pytest.mark.parametrize(
    ("date_key", "expected"),
    [
        (20240229, ["2024-02-29", "2024", "1", "2", "29", "4", "f"]),
        (20261108, ["2026-11-08", "2026", "4", "11", "8", "7", "t"]),
    ],
)
def test_date_attributes(gold: Stack, date_key: int, expected: list[str]) -> None:
    day = rows(
        gold,
        "SELECT calendar_date, year, quarter, month, day_of_month, day_of_week, is_weekend"
        f" FROM gold.dim_date WHERE date_key = {date_key};",
    )

    assert day == [expected]


def test_history_has_monthly_partitions_for_this_month_and_the_next_two(gold: Stack) -> None:
    months = rows(
        gold,
        "SELECT to_char(date_trunc('month', localtimestamp) + make_interval(months => n),"
        " 'YYYY_MM') FROM generate_series(0, 2) AS n;",
    )
    expected = [
        [fact, f"{fact}_{suffix}"]
        for fact in ("fact_order_history", "fact_order_item_history")
        for suffix in [*(month[0] for month in months), "default"]
    ]

    assert rows(gold, HISTORY_PARTITIONS) == expected


def test_history_rows_land_in_their_month_or_the_default_partition(gold: Stack) -> None:
    placed = rows(
        gold,
        rolled_back(
            order_change(1, 10, NEXT_MONTH),
            order_change(2, 10, BEFORE_ANY_PARTITION),
            item_change(1, 10, IN_TWO_MONTHS, quantity=1),
            "SELECT order_id, tableoid::regclass, to_char(_changed_at, 'YYYY_MM')"
            " FROM gold_internal.fact_order_history ORDER BY 1;",
            "SELECT order_item_id, tableoid::regclass, to_char(_changed_at, 'YYYY_MM')"
            " FROM gold_internal.fact_order_item_history;",
        ),
    )
    first, out_of_range, item = placed

    assert first[1] == f"gold_internal.fact_order_history_{first[2]}"
    assert out_of_range[1] == "gold_internal.fact_order_history_default"
    assert item[1] == f"gold_internal.fact_order_item_history_{item[2]}"


def test_new_partition_takes_over_rows_from_the_default_partition(gold: Stack) -> None:
    placed = rows(
        gold,
        rolled_back(
            order_change(1, 10, IN_SIX_MONTHS),
            "SELECT tableoid::regclass FROM gold_internal.fact_order_history;",
            "DO $$ BEGIN PERFORM gold_internal.ensure_history_partitions(6); END $$;",
            "SELECT tableoid::regclass, to_char(_changed_at, 'YYYY_MM')"
            " FROM gold_internal.fact_order_history;",
        ),
    )
    before, after = placed

    assert before == ["gold_internal.fact_order_history_default"]
    assert after[0] == f"gold_internal.fact_order_history_{after[1]}"


def test_a_replayed_history_row_is_rejected(gold: Stack) -> None:
    run = psql(gold, rolled_back(order_change(1, 10, NEXT_MONTH), order_change(1, 10, NEXT_MONTH)))

    assert run.returncode != 0
    assert "duplicate key value violates unique constraint" in run.stderr


def test_a_replayed_history_row_is_ignored_on_conflict(gold: Stack) -> None:
    count = rows(
        gold,
        rolled_back(
            order_change(1, 10, NEXT_MONTH, ignore_duplicate=True),
            order_change(1, 10, NEXT_MONTH, ignore_duplicate=True),
            "SELECT count(*) FROM gold.fact_order_history;",
        ),
    )

    assert count == [["1"]]


def test_order_versions_are_ordered_by_commit_sequence_despite_a_late_event(
    gold: Stack,
) -> None:
    versions = rows(
        gold,
        rolled_back(
            order_change(7, 500, "timestamp '2026-11-02 10:00'", "PROCESSING"),
            order_change(7, 610, "timestamp '2026-11-02 11:00'", "COMPLETED"),
            order_change(7, 420, "timestamp '2026-11-02 09:00'", "PENDING"),
            "SELECT status_key, _valid_to, _is_current"
            " FROM gold.fact_order_history ORDER BY _changed_at;",
        ),
    )

    assert versions == [
        ["1", "2026-11-02 10:00:00", "f"],
        ["2", "2026-11-02 11:00:00", "f"],
        ["4", "", "t"],
    ]


def test_item_versions_are_ordered_by_commit_sequence_despite_a_late_event(
    gold: Stack,
) -> None:
    versions = rows(
        gold,
        rolled_back(
            item_change(3, 700, "timestamp '2026-11-03 10:00'", quantity=2),
            item_change(3, 650, "timestamp '2026-11-03 09:00'", quantity=1),
            "SELECT quantity, _valid_to, _is_current"
            " FROM gold.fact_order_item_history ORDER BY _changed_at;",
        ),
    )

    assert versions == [["1", "2026-11-03 10:00:00", "f"], ["2", "", "t"]]


def test_open_orders_by_delivery_date_and_status(gold: Stack) -> None:
    assert answer(gold, "01_open_orders_by_delivery_date_and_status.sql") == [
        ["2026-11-10", "PENDING", "2"],
        ["2026-11-10", "PROCESSING", "1"],
        ["2026-11-10", "UNKNOWN", "1"],
        ["2026-11-11", "PENDING", "1"],
        ["2026-11-11", "REPROCESSING", "1"],
        ["2026-11-12", "PENDING", "1"],
        ["2026-11-13", "PROCESSING", "1"],
    ]


def test_top_delivery_dates_include_ties_for_third_place(gold: Stack) -> None:
    assert answer(gold, "02_top_delivery_dates_by_open_orders.sql") == [
        ["2026-11-10", "4"],
        ["2026-11-11", "2"],
        ["2026-11-12", "1"],
        ["2026-11-13", "1"],
    ]


def test_top_delivery_dates_leave_out_the_fourth_place_after_a_tie_for_first(
    gold: Stack,
) -> None:
    assert answer(gold, "02_top_delivery_dates_by_open_orders.sql", TIES_AT_THE_TOP) == [
        ["2026-12-01", "3"],
        ["2026-12-02", "3"],
        ["2026-12-03", "2"],
    ]


def test_top_customers_leave_out_the_fourth_place_after_a_tie_for_first(gold: Stack) -> None:
    assert answer(gold, "04_top_customers_by_pending_orders.sql", TIES_AT_THE_TOP) == [
        ["11", "", "3"],
        ["12", "", "3"],
        ["13", "", "2"],
    ]


def test_pending_items_by_product(gold: Stack) -> None:
    assert answer(gold, "03_pending_items_by_product.sql") == [
        ["10", "2", "10"],
        ["20", "1", "2"],
        ["30", "1", "8"],
    ]


def test_top_customers_include_ties_and_customers_missing_from_the_dimension(
    gold: Stack,
) -> None:
    assert answer(gold, "04_top_customers_by_pending_orders.sql") == [
        ["1", "Ada Lovelace", "3"],
        ["2", "Grace Hopper", "3"],
        ["3", "Alan Turing", "1"],
        ["4", "", "1"],
    ]


def test_freshness_is_the_newest_change_per_entity(gold: Stack) -> None:
    assert rows(gold, rolled_back(FRESHNESS_FIXTURE, FRESHNESS)) == [
        ["customers", "2026-11-09 08:00:00"],
        ["order_items", ""],
        ["orders", "2026-11-09 09:30:00"],
        ["products", ""],
    ]
