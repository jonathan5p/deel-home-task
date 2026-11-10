from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree
from pipelines.gold.dimensions import DIMENSIONS, FACTS, load_dimensions, load_facts


def test_customers_and_products_are_the_scd2_dimensions() -> None:
    assert [(d.name, d.entity.name, d.key, d.surrogate_key) for d in DIMENSIONS] == [
        ("dim_customer", "customers", "customer_id", "customer_key"),
        ("dim_product", "products", "product_id", "product_key"),
    ]


def test_the_product_price_is_loaded_from_the_misspelled_source_column() -> None:
    assert load_dimensions()["dim_product"].silver_columns["unit_price"] == "unity_price"


def test_dimension_columns_are_loaded_from_their_source_column_by_lake_name() -> None:
    customer = load_dimensions()["dim_customer"].silver_columns

    assert {name: customer[name] for name in ("customer_id", "customer_name", "_valid_from")} == {
        "customer_id": "customer_id",
        "customer_name": "customer_name",
        "_valid_from": "_changed_at",
    }


def test_dimensions_can_be_loaded_from_another_schema_folder(schemas: SchemasTree) -> None:
    assert set(load_dimensions(schemas.root)) == {"dim_customer", "dim_product"}


def test_orders_and_order_items_are_the_history_facts_with_their_current_state() -> None:
    assert [(f.name, f.current.name, f.entity.name, f.key) for f in FACTS] == [
        ("fact_order_history", "current_orders", "orders", "order_id"),
        ("fact_order_item_history", "current_order_items", "order_items", "order_item_id"),
    ]


def test_order_columns_are_looked_up_in_the_status_date_and_customer_dimensions() -> None:
    orders = load_facts()["fact_order_history"]

    lookups = [(k.column, k.source, k.dimension.name, k.fallback) for k in orders.lookups]

    assert lookups == [
        ("status_key", "status", "dim_order_status", "UNKNOWN"),
        ("order_date_key", "order_date", "dim_date", None),
        ("delivery_date_key", "delivery_date", "dim_date", None),
    ]
    assert [(a.dimension.name, a.business_key, a.columns) for a in orders.as_of] == [
        ("dim_customer", "customer_id", (("customer_key", "customer_key"),))
    ]


def test_items_take_the_product_version_and_price_valid_when_they_changed() -> None:
    (product,) = load_facts()["fact_order_item_history"].as_of

    assert product.dimension.name == "dim_product"
    assert product.fact_columns == ("product_key", "unit_price", "line_amount")


def test_facts_can_be_loaded_from_another_schema_folder(schemas: SchemasTree) -> None:
    assert set(load_facts(schemas.root)) == {"fact_order_history", "fact_order_item_history"}
