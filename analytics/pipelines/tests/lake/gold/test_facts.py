from dataclasses import replace

import pytest

from analytics.pipelines.tests.lake.gold.facts_setup import (
    CUSTOMER,
    ITEMS,
    ORDERS,
    PRODUCT,
    STATUS,
    with_column,
    without,
)
from pipelines.lake.gold.facts import FactDefinitionError, Multiplied


def test_staging_holds_the_silver_columns_of_both_tables_then_the_lookup_inputs() -> None:
    assert [column.name for column in ORDERS.staged_columns] == [
        "order_id",
        "_commit_sequence",
        "_changed_at",
        "_change_op",
        "customer_id",
        "_content_hash",
        "_is_deleted",
        "order_date",
        "delivery_date",
        "status",
    ]


def test_each_staged_column_is_loaded_from_its_silver_column_by_lake_name() -> None:
    assert ITEMS.silver_columns == {
        "order_item_id": "order_item_id",
        "_commit_sequence": "_commit_sequence",
        "_changed_at": "_changed_at",
        "_change_op": "_change_op",
        "order_id": "order_id",
        "product_id": "product_id",
        "quantity": "quantity",
        "_content_hash": "_content_hash",
        "_is_deleted": "_is_deleted",
    }
    assert ORDERS.silver_columns["status"] == "status"


def test_a_lookup_input_only_silver_has_keeps_its_source_description() -> None:
    status = ORDERS.staged_columns[-1]
    (source,) = (c for c in ORDERS.entity.columns if c.name == "status")

    assert status == source


def test_the_fact_is_named_after_its_history_in_the_internal_schema() -> None:
    assert ORDERS.name == "fact_order_history"
    assert ORDERS.relation == "gold_internal.fact_order_history"
    assert ORDERS.current_relation == "gold_internal.current_orders"


def test_the_key_comes_from_the_entity_key() -> None:
    assert ITEMS.key == "order_item_id"


def test_the_lookups_and_the_version_columns_are_the_derived_columns() -> None:
    versions = {"_is_folded", "_version_number", "_valid_to", "_is_current"}

    assert ORDERS.derived == {
        "status_key",
        "is_open",
        "order_date_key",
        "delivery_date_key",
        "customer_key",
        *versions,
    }
    assert ITEMS.derived == {"product_key", "unit_price", "line_amount", *versions}


def test_silver_input_with_every_source_column_is_accepted() -> None:
    ORDERS.check_silver(ORDERS.silver_columns.values())


def test_silver_input_without_a_source_column_is_refused() -> None:
    columns = [name for name in ORDERS.silver_columns.values() if name != "status"]

    with pytest.raises(FactDefinitionError, match="silver input has no columns named status"):
        ORDERS.check_silver(columns)


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("history", "_content_hash"),
        ("history", "_changed_at"),
        ("history", "_is_current"),
        ("current", "_is_deleted"),
    ],
)
def test_a_table_without_a_change_column_is_refused(table: str, column: str) -> None:
    gold = getattr(ORDERS, table)

    with pytest.raises(FactDefinitionError, match=f"{gold.name} has no columns named {column}"):
        replace(ORDERS, **{table: without(gold, column)})


def test_a_lookup_of_a_column_neither_table_has_is_refused() -> None:
    with pytest.raises(FactDefinitionError, match="neither table has: status_code"):
        replace(ORDERS, lookups=(*ORDERS.lookups, replace(STATUS, column="status_code")))


def test_a_lookup_in_a_dimension_without_its_code_column_is_refused() -> None:
    lookup = replace(STATUS, code="code")

    with pytest.raises(FactDefinitionError, match="dim_order_status has no columns named code"):
        replace(ORDERS, lookups=(lookup, *ORDERS.lookups[1:]))


def test_a_lookup_of_an_input_silver_does_not_have_is_refused() -> None:
    lookup = replace(STATUS, source="state")

    with pytest.raises(FactDefinitionError, match="lookups read state, which neither table"):
        replace(ORDERS, lookups=(lookup, *ORDERS.lookups[1:]))


def test_an_as_of_lookup_must_fill_history_columns() -> None:
    lookup = replace(CUSTOMER, columns=(("customer_key", "customer_key"), ("label", "x")))

    with pytest.raises(FactDefinitionError, match="does not have: label"):
        replace(ORDERS, as_of=(lookup,))


def test_an_as_of_lookup_of_a_column_the_dimension_lacks_is_refused() -> None:
    lookup = replace(CUSTOMER, columns=(("customer_key", "key"),))

    with pytest.raises(FactDefinitionError, match="dim_customer has no columns named key"):
        replace(ORDERS, as_of=(lookup,))


def test_a_product_of_a_column_the_lookup_does_not_fill_is_refused() -> None:
    lookup = replace(PRODUCT, multiplied=(Multiplied("line_amount", "quantity", "price"),))

    with pytest.raises(FactDefinitionError, match="needs a looked-up column: price"):
        replace(ITEMS, as_of=(lookup,))


def test_a_column_without_a_silver_column_or_a_lookup_is_refused() -> None:
    with pytest.raises(FactDefinitionError, match="no silver column of order_items for label"):
        replace(ITEMS, current=with_column(ITEMS.current, "label"))


def test_the_current_state_table_must_hold_the_key() -> None:
    with pytest.raises(FactDefinitionError, match=r"must come from order_items\.order_item_id"):
        replace(ITEMS, current=without(ITEMS.current, "order_item_id"))


def test_the_open_flag_is_kept_only_with_the_current_state() -> None:
    assert "is_open" in {column.name for column in ORDERS.current.columns}
    assert "is_open" not in {column.name for column in ORDERS.history.columns}
    assert "is_open" not in {column.name for column in ORDERS.staged_columns}
