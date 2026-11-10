from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree
from pipelines.gold.dimensions import DIMENSIONS, load_dimensions


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
