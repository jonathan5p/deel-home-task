import pytest

pytest.importorskip("pyspark")

import duckdb

from analytics.pipelines.tests.latency.latency_setup import CHANGE, CUSTOMER, ORDER
from pipelines.latency.gold import GoldLookup, GoldTable, gold_lookup_query, gold_tables
from pipelines.latency.probe import Change, ProbeSetupError
from pipelines.latency.sql import Row


def test_every_entity_is_looked_up_in_the_gold_table_its_changes_are_appended_to() -> None:
    assert gold_tables() == {
        "customers": GoldTable("gold_internal.dim_customer", "customer_id"),
        "products": GoldTable("gold_internal.dim_product", "product_id"),
        "orders": GoldTable("gold_internal.fact_order_history", "order_id"),
        "order_items": GoldTable("gold_internal.fact_order_item_history", "order_item_id"),
    }


def test_the_gold_lookup_filters_on_the_key_and_the_commit_sequence() -> None:
    table = GoldTable("gold_internal.dim_customer", "customer_id")

    assert gold_lookup_query(table, CUSTOMER, CHANGE) == (
        'SELECT count(*) FROM "gold_internal"."dim_customer"'
        ' WHERE "customer_id" = 7 AND "_commit_sequence" = 500'
    )


def test_the_gold_lookup_sees_the_change_once_gold_internal_holds_it() -> None:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA gold_internal")
    connection.execute(
        "CREATE TABLE gold_internal.dim_customer (customer_id BIGINT, _commit_sequence BIGINT)"
    )
    connection.execute("INSERT INTO gold_internal.dim_customer VALUES (7, 500)")

    def query(sql: str) -> list[Row]:
        return connection.execute(sql).fetchall()

    lookup = GoldLookup(gold_tables(), query)
    later = Change(501, CHANGE.changed_at, CHANGE.captured_at, CHANGE.published_at)

    assert lookup.visible(CUSTOMER, CHANGE)
    assert not lookup.visible(CUSTOMER, later)


def test_an_entity_without_a_gold_table_cannot_be_looked_up() -> None:
    lookup = GoldLookup({}, lambda sql: [(1,)])

    with pytest.raises(ProbeSetupError, match="orders"):
        lookup.visible(ORDER, CHANGE)
