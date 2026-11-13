from collections.abc import Iterator

import pytest

from tests.infra.gold_reader_setup import as_api_reader
from tests.infra.gold_reset_setup import (
    CATALOG_TABLE,
    EXTRA_STATUS,
    GoldReset,
    lake_gold_entries,
    lake_has,
    remove_catalog_table,
    run_gold_reset_scenario,
)
from tests.infra.stack import Stack


@pytest.fixture(scope="module")
def reset(stack: Stack) -> Iterator[GoldReset]:
    try:
        yield run_gold_reset_scenario(stack)
    finally:
        remove_catalog_table(stack)


def test_gold_reset_refuses_without_confirm(reset: GoldReset) -> None:
    assert reset.refused.returncode != 0
    assert "CONFIRM=1" in reset.refused.stderr


def test_gold_reset_succeeds_and_can_run_again(reset: GoldReset) -> None:
    assert reset.first.returncode == 0, reset.first.stderr
    assert reset.second.returncode == 0, reset.second.stderr


def test_gold_reset_recreates_the_gold_schemas_empty(stack: Stack, reset: GoldReset) -> None:
    statuses = stack.sql(
        "analytics",
        f"SELECT count(*) FROM gold_internal.dim_order_status WHERE status_key = {EXTRA_STATUS};",
    )
    views = stack.sql("analytics", "SELECT count(*) FROM pg_views WHERE schemaname = 'gold';")

    assert statuses == "0"
    assert views != "0"


def test_gold_reset_drops_the_gold_iceberg_tables_their_former_names_and_the_views(
    stack: Stack, reset: GoldReset
) -> None:
    dropped = "Dropped the gold tables in lake.gold_internal and the gold views in lake.gold"

    assert dropped in reset.first.stdout
    assert dropped in reset.second.stdout
    assert lake_gold_entries(stack) == "0"


def test_gold_reset_deletes_only_the_gold_checkpoints(stack: Stack, reset: GoldReset) -> None:
    assert not lake_has(stack, "checkpoints", "gold")
    assert lake_has(stack, "checkpoints/silver/customers/offsets", "0")


def test_gold_reset_keeps_the_iceberg_catalog(stack: Stack, reset: GoldReset) -> None:
    tables = stack.sql(
        "iceberg_catalog", f"SELECT count(*) FROM pg_tables WHERE tablename = '{CATALOG_TABLE}';"
    )

    assert tables == "1"


def test_gold_reset_keeps_the_api_login_reading_the_gold_views(
    stack: Stack, reset: GoldReset
) -> None:
    run = as_api_reader(stack, "SELECT count(*) FROM gold.top_customers_by_pending_orders;")

    assert run.returncode == 0, run.stderr
