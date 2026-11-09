import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from analytics.pipelines.tests.bronze.parse_samples import AUDIT_JSON
from pipelines.bronze.entities import (
    CUSTOMERS,
    ENTITIES,
    ORDER_ITEMS,
    ORDERS,
    PRODUCTS,
    VARIABLE_SCALE_DECIMAL,
    Entity,
    UnknownEntityError,
    entity_for_topic,
)
from pipelines.schemas.definitions import load_entity


def test_there_is_one_entity_per_source_table() -> None:
    assert [entity.name for entity in ENTITIES] == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]


def test_entities_are_built_from_their_definitions() -> None:
    assert CUSTOMERS.definition == load_entity("customers")
    assert CUSTOMERS.columns == CUSTOMERS.definition.all_columns


@pytest.mark.parametrize(
    ("entity", "key", "content"),
    [
        (CUSTOMERS, "customer_id", ("customer_name", "is_active", "customer_address")),
        (PRODUCTS, "product_id", ("product_name", "barcode", "unity_price", "is_active")),
        (ORDERS, "order_id", ("order_date", "delivery_date", "customer_id", "status")),
        (ORDER_ITEMS, "order_item_id", ("order_id", "product_id", "quantity")),
    ],
)
def test_each_entity_splits_its_columns_into_key_content_and_audit(
    entity: Entity, key: str, content: tuple[str, ...]
) -> None:
    assert entity.key == key
    assert entity.content_columns == content
    assert entity.audit_columns == tuple(AUDIT_JSON)


@pytest.mark.parametrize(
    ("entity", "not_null"),
    [
        (CUSTOMERS, ("customer_id", "customer_name", "is_active")),
        (PRODUCTS, ("product_id", "product_name", "barcode", "unity_price")),
        (ORDERS, ("order_id",)),
        (ORDER_ITEMS, ("order_item_id",)),
    ],
)
def test_not_null_columns_follow_the_source_ddl(entity: Entity, not_null: tuple[str, ...]) -> None:
    assert entity.not_null_columns == not_null


def test_the_row_schema_is_the_json_debezium_emits() -> None:
    audit = [
        StructField("updated_at", LongType()),
        StructField("updated_by", LongType()),
        StructField("created_at", LongType()),
        StructField("created_by", LongType()),
    ]

    assert CUSTOMERS.row_schema == StructType(
        [
            StructField("customer_id", LongType()),
            StructField("customer_name", StringType()),
            StructField("is_active", BooleanType()),
            StructField("customer_address", StringType()),
            *audit,
        ]
    )
    assert PRODUCTS.row_schema["unity_price"].dataType == VARIABLE_SCALE_DECIMAL
    assert ORDERS.row_schema["order_date"].dataType == IntegerType()
    assert ORDER_ITEMS.row_schema["quanity"].dataType == IntegerType()


def test_a_renamed_column_keeps_its_source_name_in_the_row_schema() -> None:
    assert "quanity" in ORDER_ITEMS.row_schema.fieldNames()
    assert "quantity" in ORDER_ITEMS.content_columns


@pytest.mark.parametrize("entity", ENTITIES, ids=lambda entity: entity.name)
def test_the_entity_is_resolved_from_the_last_topic_segment(entity: Entity) -> None:
    assert entity_for_topic(f"test.operations.{entity.name}") is entity


def test_an_unknown_table_is_reported_with_the_known_ones() -> None:
    with pytest.raises(UnknownEntityError, match=r"'test\.operations\.invoices'") as error:
        entity_for_topic("test.operations.invoices")

    assert "customers, order_items, orders, products" in str(error.value)


def test_the_entities_to_resolve_from_can_be_injected() -> None:
    with pytest.raises(UnknownEntityError):
        entity_for_topic("test.operations.orders", entities=(CUSTOMERS,))
