import pytest

from analytics.pipelines.tests.schemas.definitions_setup import DELETE, Location, SchemasTree
from analytics.pipelines.tests.schemas.gold_definitions_setup import write_fact
from pipelines.schemas.definitions import (
    ColumnDefinition,
    ColumnType,
    DefinitionError,
    load_entity,
    load_table_templates,
)
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

GOLD_TABLES = {
    "current_order_items",
    "current_orders",
    "data_freshness",
    "dim_customer",
    "dim_customer_versions",
    "dim_date",
    "dim_order_status",
    "dim_product",
    "dim_product_versions",
    "fact_order_history",
    "fact_order_item_history",
}


def test_every_gold_relation_has_a_schema() -> None:
    assert set(load_gold_tables()) == GOLD_TABLES


def test_every_gold_column_is_described() -> None:
    columns = [column for table in load_gold_tables().values() for column in table.columns]

    assert all(column.description.strip() for column in columns)


def test_a_gold_table_keeps_the_field_order_of_its_schema() -> None:
    current_orders = load_gold_tables()["current_orders"]

    assert [column.name for column in current_orders.columns] == [
        "order_id",
        "status_key",
        "customer_id",
        "order_date",
        "delivery_date",
        "order_date_key",
        "delivery_date_key",
        "_commit_sequence",
        "_changed_at",
        "_is_deleted",
    ]


def test_gold_columns_take_the_description_of_the_column_they_come_from() -> None:
    tables = load_gold_tables()
    columns = {column.name: column for column in tables["fact_order_item_history"].columns}
    order_items = {column.lake_name: column for column in load_entity("order_items").columns}

    assert columns["quantity"].description == order_items["quantity"].description
    assert columns["quantity"].not_null is False


def test_gold_metadata_columns_match_their_bronze_definition() -> None:
    bronze = {column.name: column for column in load_table_templates()["bronze"].fields}
    fact = load_gold_tables()["fact_order_history"]

    for name in ("_commit_sequence", "_change_op", "_changed_at"):
        assert fact.column(name) == bronze[name]


def test_a_gold_column_can_take_the_description_of_another_gold_column() -> None:
    tables = load_gold_tables()
    fact = {column.name: column for column in tables["fact_order_history"].columns}
    current = {column.name: column for column in tables["current_orders"].columns}

    assert current["delivery_date_key"].description == fact["delivery_date_key"].description


def test_a_gold_table_without_a_public_doc_describes_its_view_like_its_table() -> None:
    tables = load_gold_tables()

    assert tables["dim_date"].view_description == tables["dim_date"].description
    assert "not deleted" in tables["current_orders"].view_description
    assert "deleted ones included" in tables["current_orders"].description


def test_gold_schemas_can_be_loaded_from_another_folder(schemas: SchemasTree) -> None:
    write_fact(schemas)

    fact = load_gold_tables(schemas.root)["fact_things"]

    assert fact == GoldTable(
        name="fact_things",
        description="Changes to things",
        public_description="Changes to things, as consumers see them",
        columns=(
            ColumnDefinition("thing_key", ColumnType.LONG, "Surrogate key of the thing", True),
            ColumnDefinition("thing_id", ColumnType.LONG, "Thing identifier", True),
            ColumnDefinition("thing_name", ColumnType.STRING, "Name of the thing"),
            ColumnDefinition("_commit_sequence", ColumnType.LONG, "Commit position", True),
            ColumnDefinition("same_key", ColumnType.LONG, "Surrogate key of the thing", True),
        ),
        references=(
            ("thing_id", "things.thing_id"),
            ("thing_name", "things.thing_name"),
            ("same_key", "gold.fact_things.thing_key"),
        ),
    )


def test_a_gold_table_keeps_where_each_referencing_column_comes_from() -> None:
    dim_product = load_gold_tables()["dim_product"]

    assert dim_product.reference("product_name") == "products.product_name"
    assert dim_product.reference("unit_price") is None


@pytest.mark.parametrize(
    ("path", "value", "problem"),
    [
        (("type",), "enum", 'the schema must be of "type": "record"'),
        (("name",), "fact_stuff", "record fact_stuff must be defined in fact_stuff.avsc"),
        (("doc",), DELETE, "record fact_things needs a non-blank doc"),
        (("public_doc",), " ", "record fact_things needs a non-blank public_doc"),
        (("key",), "thing_id", "unknown properties of record fact_things: key"),
        (("fields",), [], "fields of record fact_things must be a non-empty list"),
        (("fields", 0), "thing_key", "every field must be a JSON object"),
        (("fields", 0, "type"), "text", 'unsupported type "text" of field thing_key'),
        (("fields", 0, "default"), 0, "field thing_key is not nullable and takes no default"),
        (("fields", 2, "default"), DELETE, 'nullable field thing_name needs "default": null'),
        (("fields", 0, "doc"), DELETE, "field thing_key needs either a doc or a from, not both"),
        (("fields", 1, "doc"), "Id", "field thing_id needs either a doc or a from, not both"),
        (("fields", 1, "from"), " ", "field thing_id needs a non-blank from"),
        (("fields", 1, "renamed_to"), "id", "unknown properties of field thing_id: renamed_to"),
        (("fields", 1, "from"), "things.thing_nmae", "from things.thing_nmae of field thing_id"),
        (("fields", 1, "from"), "nope.thing_id", "names no column with its own doc"),
        (("fields", 1, "from"), "metadata._commit_sequence", "names no column with its own doc"),
        (("fields", 4, "from"), "gold.fact_things.thing_id", "names no column with its own doc"),
        (("fields", 1, "from"), "things.thing_name", "thing_id is long but things.thing_name"),
        (("fields", 4, "name"), "thing_id", "two fields are named thing_id"),
    ],
)
def test_a_broken_gold_schema_is_refused_naming_its_file(
    schemas: SchemasTree, path: Location, value: object, problem: str
) -> None:
    write_fact(schemas, path, value)

    with pytest.raises(DefinitionError, match=r"fact_things\.avsc: ") as error:
        load_gold_tables(schemas.root)

    assert problem in str(error.value)
