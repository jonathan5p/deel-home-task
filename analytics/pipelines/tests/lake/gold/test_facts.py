from dataclasses import replace

import pytest

from analytics.pipelines.tests.lake.gold.facts_setup import (
    CUSTOMER,
    DELIVERY_DATE,
    IS_OPEN,
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


def test_the_staging_table_is_named_after_the_history_and_describes_both_tables() -> None:
    staging = ORDERS.staging_table

    assert ORDERS.name == "fact_order_history"
    assert ORDERS.staging_relation == "gold_internal.stage_fact_order_history"
    assert staging.columns == ORDERS.staged_columns
    assert "gold_internal.fact_order_history and gold_internal.current_orders" in (
        staging.description
    )


def test_the_key_and_the_history_key_come_from_the_entity_key() -> None:
    assert ITEMS.key == "order_item_id"
    assert ITEMS.history_key == ("order_item_id", "_commit_sequence", "_changed_at")


def test_the_lookups_fill_the_derived_columns() -> None:
    assert ORDERS.derived == {
        "status_key",
        "is_open",
        "order_date_key",
        "delivery_date_key",
        "customer_key",
    }
    assert ITEMS.derived == {"product_key", "unit_price", "line_amount"}


def test_silver_input_with_every_source_column_is_accepted() -> None:
    ORDERS.check_silver(ORDERS.silver_columns.values())


def test_silver_input_without_a_source_column_is_refused() -> None:
    columns = [name for name in ORDERS.silver_columns.values() if name != "status"]

    with pytest.raises(FactDefinitionError, match="silver input has no columns named status"):
        ORDERS.check_silver(columns)


def test_every_change_is_appended_to_history_unless_already_stored() -> None:
    statement = ORDERS.history_statement()
    targets = statement.split("(\n", 1)[1].split("\n)", 1)[0]

    assert statement.startswith("INSERT INTO gold_internal.fact_order_history (\n")
    assert targets.split(",\n") == [f"    {column.name}" for column in ORDERS.history.columns]
    assert "FROM gold_internal.stage_fact_order_history AS fact_row\n" in statement
    assert statement.endswith("ON CONFLICT (order_id, _commit_sequence, _changed_at) DO NOTHING")


def test_the_history_select_takes_staged_columns_and_looks_up_the_others() -> None:
    statement = ORDERS.history_statement()

    assert "    fact_row.customer_id,\n    dim_customer.customer_key,\n" in statement
    assert f"    {STATUS.expression('fact_row')},\n" in statement
    assert "    fact_row._content_hash,\n    fact_row._is_deleted\n" in statement


def test_an_unknown_or_null_status_falls_back_to_the_unknown_key() -> None:
    assert STATUS.expression("fact_row") == (
        "coalesce((SELECT lookup.status_key FROM gold_internal.dim_order_status AS lookup"
        " WHERE lookup.status_code = fact_row.status),"
        " (SELECT lookup.status_key FROM gold_internal.dim_order_status AS lookup"
        " WHERE lookup.status_code = 'UNKNOWN'))"
    )


def test_a_date_outside_dim_date_or_null_has_a_null_key() -> None:
    assert DELIVERY_DATE.expression("fact_row") == (
        "(SELECT lookup.date_key FROM gold_internal.dim_date AS lookup"
        " WHERE lookup.calendar_date = fact_row.delivery_date)"
    )


def test_a_fallback_code_is_quoted_as_a_postgres_string() -> None:
    assert "= 'it''s'" in replace(STATUS, fallback="it's").expression("fact_row")


def test_a_version_is_looked_up_as_of_the_change_or_is_the_first_for_an_older_one() -> None:
    assert PRODUCT.join("fact_row").splitlines() == [
        "LEFT JOIN LATERAL (",
        "    SELECT version.product_key, version.unit_price",
        "    FROM gold_internal.dim_product_versions AS version",
        "    WHERE version.product_id = fact_row.product_id",
        "      AND NOT version._is_folded",
        "      AND NOT version._is_deleted",
        "      AND (",
        "          version._valid_from <= fact_row._changed_at",
        "          OR NOT EXISTS (",
        "              SELECT 1",
        "              FROM gold_internal.dim_product AS earlier",
        "              WHERE earlier.product_id = version.product_id",
        "                AND earlier._commit_sequence < version._commit_sequence",
        "          )",
        "      )",
        "      AND (version._valid_to IS NULL OR fact_row._changed_at < version._valid_to)",
        "    ORDER BY version._commit_sequence DESC",
        "    LIMIT 1",
        ") AS dim_product ON true",
    ]


def test_the_line_amount_is_the_quantity_times_the_looked_up_price() -> None:
    assert PRODUCT.expressions("fact_row") == {
        "product_key": "dim_product.product_key",
        "unit_price": "dim_product.unit_price",
        "line_amount": "fact_row.quantity * dim_product.unit_price",
    }
    assert "    fact_row.quantity * dim_product.unit_price,\n" in ITEMS.history_statement()


def test_the_merge_and_the_re_resolution_share_the_as_of_lookup() -> None:
    (statement,) = ITEMS.resolve_statements("dim_product")
    indented = "\n".join(f"    {line}" for line in PRODUCT.join("fact_row").splitlines())

    assert PRODUCT.join("fact_row") in ITEMS.history_statement()
    assert indented in statement


def test_the_current_state_is_merged_from_the_latest_staged_change_of_each_key() -> None:
    statement = ORDERS.current_statement()

    assert statement.startswith("MERGE INTO gold_internal.current_orders AS target\nUSING (\n")
    assert (
        "    FROM (\n"
        "        SELECT DISTINCT ON (order_id) *\n"
        "        FROM gold_internal.stage_fact_order_history\n"
        "        ORDER BY order_id, _commit_sequence DESC\n"
        "    ) AS fact_row\n"
        ") AS source\n"
        "ON target.order_id = source.order_id\n"
    ) in statement
    assert f"        {STATUS.expression('fact_row')} AS status_key,\n" in statement
    assert "LATERAL" not in statement


def test_a_current_row_is_only_replaced_by_a_newer_change() -> None:
    statement = ORDERS.current_statement()
    matched = statement.split("WHEN MATCHED", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    replaced = [c.name for c in ORDERS.current.columns if c.name != "order_id"]

    assert matched.splitlines() == [
        " AND source._commit_sequence > target._commit_sequence THEN",
        "    UPDATE SET",
        *(f"        {name} = source.{name}," for name in replaced[:-1]),
        f"        {replaced[-1]} = source.{replaced[-1]}",
    ]


def test_a_new_key_is_inserted_with_every_current_column() -> None:
    inserted = ORDERS.current_statement().split("WHEN NOT MATCHED THEN\n", 1)[1]
    names = [c.name for c in ORDERS.current.columns]

    assert inserted.splitlines() == [
        "    INSERT (",
        *(f"        {name}," for name in names[:-1]),
        f"        {names[-1]}",
        "    )",
        "    VALUES (",
        *(f"        source.{name}," for name in names[:-1]),
        f"        source.{names[-1]}",
        "    )",
    ]


def test_the_merge_appends_history_then_applies_the_current_state() -> None:
    assert ITEMS.merge_statements() == (ITEMS.history_statement(), ITEMS.current_statement())


def test_a_dimension_merge_re_resolves_the_history_rows_of_the_keys_it_touched() -> None:
    (statement,) = ITEMS.resolve_statements("dim_product")

    assert statement.startswith("WITH resolved AS (\n")
    assert "        fact_row.quantity * dim_product.unit_price AS line_amount\n" in statement
    assert "    FROM gold_internal.fact_order_item_history AS fact_row\n" in statement
    assert (
        "    WHERE fact_row.product_id IN (SELECT product_id FROM gold_internal.stage_dim_product)"
    ) in statement


def test_only_rows_whose_looked_up_values_change_are_updated() -> None:
    (statement,) = ORDERS.resolve_statements("dim_customer")

    assert statement.endswith(
        "UPDATE gold_internal.fact_order_history AS stored\n"
        "SET\n"
        "    customer_key = resolved.customer_key\n"
        "FROM resolved\n"
        "WHERE stored.order_id = resolved.order_id\n"
        "  AND stored._commit_sequence = resolved._commit_sequence\n"
        "  AND stored._changed_at = resolved._changed_at\n"
        "  AND (stored.customer_key) IS DISTINCT FROM (resolved.customer_key)"
    )


def test_a_dimension_the_fact_does_not_look_up_re_resolves_nothing() -> None:
    assert ORDERS.resolve_statements("dim_product") == ()
    assert ITEMS.resolve_statements("fact_order_item_history") == ()


@pytest.mark.parametrize(
    ("table", "column"),
    [("history", "_content_hash"), ("history", "_changed_at"), ("current", "_is_deleted")],
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


def test_an_order_is_open_when_its_status_is_open_and_unknown_statuses_count_as_open() -> None:
    assert IS_OPEN.expression("fact_row") == (
        "coalesce((SELECT lookup.is_open FROM gold_internal.dim_order_status AS lookup"
        " WHERE lookup.status_code = fact_row.status),"
        " (SELECT lookup.is_open FROM gold_internal.dim_order_status AS lookup"
        " WHERE lookup.status_code = 'UNKNOWN'))"
    )


def test_the_open_flag_is_kept_only_with_the_current_state_and_follows_each_newer_change() -> None:
    current = ORDERS.current_statement()

    assert f"        {IS_OPEN.expression('fact_row')} AS is_open,\n" in current
    assert "        is_open = source.is_open,\n" in current
    assert "is_open" not in ORDERS.history_statement()
    assert "is_open" not in {column.name for column in ORDERS.staged_columns}
