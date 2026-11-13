from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.stored import WriteMode


def test_the_stored_tables_are_the_dimensions_and_each_facts_history_and_current_state() -> None:
    assert list(STORED) == [
        "dim_order_status",
        "dim_date",
        "dim_customer",
        "dim_product",
        "fact_order_history",
        "fact_order_item_history",
        "current_orders",
        "current_order_items",
    ]


def test_each_table_is_merged_as_its_rows_change() -> None:
    modes = {name: table.mode for name, table in STORED.items()}

    assert modes["dim_customer"] is WriteMode.MERGE
    assert modes["fact_order_history"] is WriteMode.MERGE
    assert modes["current_orders"] is WriteMode.MERGE_NEWER


def test_versions_are_keyed_by_their_business_key_and_commit_sequence() -> None:
    assert STORED["dim_product"].keys == ("product_id", "_commit_sequence")
    assert STORED["dim_product"].unique == ("product_key",)
    assert STORED["fact_order_item_history"].keys == ("order_item_id", "_commit_sequence")


def test_a_history_table_keeps_its_change_time_in_the_postgresql_key() -> None:
    history = STORED["fact_order_history"]

    assert history.history
    assert history.postgres_key == ("order_id", "_commit_sequence", "_changed_at")
    assert STORED["current_orders"].postgres_key == ("order_id",)


def test_a_history_table_stores_none_of_its_view_only_columns() -> None:
    columns = STORED["fact_order_item_history"].columns

    assert "product_id" in columns
    assert not {"product_key", "product_name", "unit_price", "line_amount"} & set(columns)


def test_every_staged_column_takes_nulls() -> None:
    staging = STORED["current_orders"].staging_table

    assert staging.name == "stage_current_orders"
    assert [column.name for column in staging.columns] == list(STORED["current_orders"].columns)
    assert not any(column.not_null for column in staging.columns)
    assert "gold_internal.current_orders" in staging.description
