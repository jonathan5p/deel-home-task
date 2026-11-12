from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.stored import WriteMode


def test_a_batch_writes_the_dimensions_before_the_facts_and_current_state_before_reports() -> None:
    assert list(STORED) == [
        "dim_order_status",
        "dim_date",
        "dim_customer",
        "dim_product",
        "fact_order_history",
        "fact_order_item_history",
        "current_orders",
        "current_order_items",
        "open_orders_by_delivery_date_and_status",
        "pending_orders_by_customer",
        "pending_items_by_product",
        "top_delivery_dates_by_open_orders",
        "top_customers_by_pending_orders",
    ]


def test_each_table_is_written_as_its_rows_change() -> None:
    modes = {name: table.mode for name, table in STORED.items()}

    assert modes["dim_customer"] is WriteMode.MERGE
    assert modes["fact_order_history"] is WriteMode.MERGE
    assert modes["current_orders"] is WriteMode.MERGE_NEWER
    assert modes["pending_items_by_product"] is WriteMode.RECOUNT
    assert modes["top_customers_by_pending_orders"] is WriteMode.REPLACE


def test_versions_are_keyed_by_their_business_key_and_commit_sequence() -> None:
    assert STORED["dim_product"].keys == ("product_id", "_commit_sequence")
    assert STORED["dim_product"].unique == ("product_key",)
    assert STORED["fact_order_item_history"].keys == ("order_item_id", "_commit_sequence")


def test_a_history_table_keeps_its_change_time_in_the_postgresql_key() -> None:
    history = STORED["fact_order_history"]

    assert history.history
    assert history.postgres_key == ("order_id", "_commit_sequence", "_changed_at")
    assert STORED["current_orders"].postgres_key == ("order_id",)


def test_a_recounted_table_stages_its_removed_flag_and_every_staged_column_takes_nulls() -> None:
    staging = STORED["pending_orders_by_customer"].staging_table

    assert staging.name == "stage_pending_orders_by_customer"
    assert [column.name for column in staging.columns] == [
        "customer_id",
        "pending_orders",
        "_is_removed",
    ]
    assert not any(column.not_null for column in staging.columns)
    assert "gold_internal.pending_orders_by_customer" in staging.description
