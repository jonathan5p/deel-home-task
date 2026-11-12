import subprocess

import pytest

from pipelines.schemas.definitions import POSTGRES_TYPES
from pipelines.schemas.gold_definitions import load_gold_tables
from tests.infra.gold_fixtures import (
    BUSINESS_VIEWS,
    CURRENT_STATE,
    DELETED_ROWS,
    FRESHNESS,
    FRESHNESS_FIXTURE,
    GOLD_DDL,
    GOLD_QUERIES,
    INTERNAL_TABLES,
    PUBLIC_PIPELINE_ONLY_COLUMNS,
    PUBLIC_VIEWS,
    STAGING_TABLES,
    UNDESCRIBED_COLUMNS,
    UNDESCRIBED_INDEXES,
    UNDESCRIBED_RELATIONS,
    UNDESCRIBED_SCHEMAS,
    column_types,
    item_change,
    order_change,
    psql,
    rolled_back,
    rows,
    top_level_relations,
    type_names,
)
from tests.infra.stack import Stack

NEXT_MONTH = "date_trunc('month', localtimestamp) + interval '1 month 2 days'"


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


def test_internal_schema_holds_only_the_stored_tables_their_staging_and_the_sync_state(
    gold: Stack,
) -> None:
    relations = dict(rows(gold, top_level_relations("gold_internal")))

    assert set(relations) == INTERNAL_TABLES | STAGING_TABLES
    assert set(relations.values()) <= {"r", "p"}


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
        ["orders", "0", "1"],
        ["order_items", "0", "1"],
    ]


def test_every_schema_table_and_column_has_a_description(gold: Stack) -> None:
    assert rows(gold, UNDESCRIBED_SCHEMAS) == []
    assert rows(gold, UNDESCRIBED_RELATIONS) == []
    assert rows(gold, UNDESCRIBED_COLUMNS) == []


def test_every_index_outside_a_constraint_has_a_description(gold: Stack) -> None:
    assert rows(gold, UNDESCRIBED_INDEXES) == []


@pytest.mark.parametrize("view", BUSINESS_VIEWS)
def test_business_views_have_the_columns_and_types_of_their_schema(gold: Stack, view: str) -> None:
    columns = load_gold_tables()[view].columns
    types = rows(gold, type_names(POSTGRES_TYPES[column.type] for column in columns))[0]

    assert rows(gold, column_types(f"gold.{view}")) == [
        [column.name, type_name] for column, type_name in zip(columns, types, strict=True)
    ]


def test_no_index_is_created_beyond_the_keys(gold: Stack) -> None:
    indexes = rows(
        gold,
        "SELECT count(*) FROM pg_indexes WHERE schemaname = 'gold_internal'"
        " AND indexname NOT LIKE '%_pkey' AND indexname NOT LIKE '%_key';",
    )

    assert indexes == [["0"]]


def test_history_rows_land_in_the_default_partition(gold: Stack) -> None:
    placed = rows(
        gold,
        rolled_back(
            order_change(1, 10, NEXT_MONTH),
            item_change(1, 10, NEXT_MONTH, quantity=1),
            "SELECT tableoid::regclass FROM gold_internal.fact_order_history;",
            "SELECT tableoid::regclass FROM gold_internal.fact_order_item_history;",
        ),
    )

    assert placed == [
        ["gold_internal.fact_order_history_default"],
        ["gold_internal.fact_order_item_history_default"],
    ]


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


@pytest.mark.parametrize(
    ("query_file", "expected"),
    [
        (
            "01_open_orders_by_delivery_date_and_status.sql",
            [
                ["2026-11-10", "PENDING", "2"],
                ["2026-11-10", "UNKNOWN", "1"],
                ["2026-11-11", "REPROCESSING", "1"],
                ["", "PENDING", "1"],
            ],
        ),
        (
            "02_top_delivery_dates_by_open_orders.sql",
            [["2026-11-10", "3"], ["2026-11-11", "1"], ["", "1"]],
        ),
        ("03_pending_items_by_product.sql", [["10", "2", "10"], ["30", "1", "8"], ["", "1", ""]]),
        (
            "04_top_customers_by_pending_orders.sql",
            [["1", "Ada Lovelace", "3"], ["2", "Grace Hopper", "3"], ["4", "", "1"]],
        ),
    ],
)
def test_the_business_questions_read_the_report_tables_as_they_are(
    gold: Stack, query_file: str, expected: list[list[str]]
) -> None:
    query = (GOLD_QUERIES / query_file).read_text()

    assert rows(gold, rolled_back(CURRENT_STATE, query)) == expected


@pytest.mark.parametrize("dimension", ["dim_customer", "dim_product"])
def test_a_surrogate_key_collision_is_refused(gold: Stack, dimension: str) -> None:
    key, business_key = f"{dimension[4:]}_key", f"{dimension[4:]}_id"
    version = (
        f"INSERT INTO gold_internal.{dimension} ({key}, {business_key}, _commit_sequence,"
        " _content_hash, _is_deleted, _valid_from, _is_folded, _version_number, _valid_to,"
        " _is_current) VALUES (77, {id}, {sequence}, 'h', false, now(), false, 1, NULL, true);"
    )
    run = psql(
        gold,
        rolled_back(version.format(id=1, sequence=10), version.format(id=2, sequence=20)),
    )

    assert run.returncode != 0
    assert f"{dimension}_{key}_key" in run.stderr


def test_a_recounted_report_key_is_unique_with_its_nulls(gold: Stack) -> None:
    run = psql(
        gold,
        rolled_back(
            "INSERT INTO gold_internal.pending_items_by_product VALUES (NULL, 1, 1);",
            "INSERT INTO gold_internal.pending_items_by_product VALUES (NULL, 2, 2);",
        ),
    )

    assert run.returncode != 0
    assert "duplicate key value violates unique constraint" in run.stderr


def test_freshness_is_the_newest_change_per_entity(gold: Stack) -> None:
    assert rows(gold, rolled_back(FRESHNESS_FIXTURE, FRESHNESS)) == [
        ["customers", "2026-11-09 08:00:00"],
        ["order_items", ""],
        ["orders", "2026-11-09 09:30:00"],
        ["products", ""],
    ]
