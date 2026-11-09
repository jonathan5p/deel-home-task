import json
from pathlib import Path

import pytest
from fastavro import parse_schema

from analytics.pipelines.tests.schemas.definitions_setup import (
    AUDIT_COLUMNS,
    DELETE,
    Location,
    SchemasTree,
    schema_files,
)
from pipelines.schemas.definitions import (
    AVRO_TYPES,
    SCHEMAS,
    ColumnDefinition,
    ColumnType,
    DefinitionError,
    EntityDefinition,
    TableTemplate,
    load_entities,
    load_entity,
    load_metadata_columns,
    load_shared_columns,
    load_table_templates,
)

SHIPPED = schema_files(Path(str(SCHEMAS)))


def test_the_schemas_folder_holds_the_shared_sources_and_tables_schemas() -> None:
    root = Path(str(SCHEMAS))

    assert [str(file.relative_to(root)) for file in SHIPPED] == [
        "audit.avsc",
        "metadata.avsc",
        "sources/customers.avsc",
        "sources/order_items.avsc",
        "sources/orders.avsc",
        "sources/products.avsc",
        "tables/bronze.avsc",
        "tables/bronze_quarantine.avsc",
    ]


@pytest.mark.parametrize("file", SHIPPED, ids=lambda file: file.name)
def test_every_shipped_schema_is_valid_avro(file: Path) -> None:
    parse_schema(json.loads(file.read_text(encoding="utf-8")))


@pytest.mark.parametrize("entity", load_entities(), ids=lambda entity: entity.name)
def test_a_source_schema_with_its_included_fields_is_one_valid_avro_record(
    entity: EntityDefinition,
) -> None:
    source = json.loads((Path(str(SCHEMAS)) / "sources" / f"{entity.name}.avsc").read_text())
    audit = json.loads((Path(str(SCHEMAS)) / "audit.avsc").read_text())

    parse_schema({**source, "fields": [*source["fields"], *audit["fields"]]})


def test_every_column_type_has_an_avro_type() -> None:
    assert set(AVRO_TYPES) == set(ColumnType)


def test_every_source_table_has_an_entity_ordered_by_name() -> None:
    assert [entity.name for entity in load_entities()] == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]


def test_an_entity_keeps_the_field_order_of_its_schema_then_the_included_columns() -> None:
    customers = load_entity("customers")

    assert customers.key == "customer_id"
    assert customers.description == "Customers who place orders"
    assert [column.name for column in customers.columns] == [
        "customer_id",
        "customer_name",
        "is_active",
        "customer_address",
    ]
    assert tuple(column.name for column in customers.included_columns) == AUDIT_COLUMNS
    assert customers.all_columns == (*customers.columns, *customers.included_columns)


def test_columns_carry_their_type_description_and_nullability() -> None:
    columns = {column.name: column for column in load_entity("products").columns}

    assert columns["unity_price"] == ColumnDefinition(
        name="unity_price",
        type=ColumnType.DECIMAL,
        description="Unit price of the product, decoded from Debezium's variable-scale decimal",
        not_null=True,
    )
    assert columns["is_active"].not_null is False


@pytest.mark.parametrize(
    ("entity", "column", "column_type"),
    [
        ("orders", "order_id", ColumnType.LONG),
        ("orders", "order_date", ColumnType.DATE),
        ("orders", "status", ColumnType.STRING),
        ("customers", "is_active", ColumnType.BOOLEAN),
        ("order_items", "quanity", ColumnType.INT),
        ("products", "unity_price", ColumnType.DECIMAL),
    ],
)
def test_avro_types_and_logical_types_map_to_column_types(
    entity: str, column: str, column_type: ColumnType
) -> None:
    (definition,) = (c for c in load_entity(entity).columns if c.name == column)

    assert definition.type is column_type


def test_a_misspelled_source_column_is_renamed_in_the_lake() -> None:
    (quanity,) = (c for c in load_entity("order_items").columns if c.name == "quanity")

    assert quanity.renamed_to == "quantity"
    assert quanity.lake_name == "quantity"


def test_a_column_that_is_not_renamed_keeps_its_name_in_the_lake() -> None:
    column = ColumnDefinition("status", ColumnType.STRING, "Status")

    assert column.lake_name == "status"


def test_the_audit_columns_are_shared_by_every_entity() -> None:
    audit = load_shared_columns("audit")

    assert tuple(column.name for column in audit) == AUDIT_COLUMNS
    assert [column.type for column in audit] == [
        ColumnType.TIMESTAMP_MILLIS,
        ColumnType.LONG,
        ColumnType.TIMESTAMP_MILLIS,
        ColumnType.LONG,
    ]
    assert not any(column.not_null for column in audit)
    assert all(entity.included_columns == audit for entity in load_entities())


def test_metadata_columns_are_described_by_name() -> None:
    columns = load_metadata_columns()

    assert columns["_commit_sequence"] == ColumnDefinition(
        name="_commit_sequence",
        type=ColumnType.LONG,
        description=(
            "Position of the change in the source's commit log (Debezium source.lsn);"
            " increases in commit order"
        ),
        not_null=True,
    )
    assert {"_valid_from", "_valid_to", "_is_current", "_is_deleted", "key", "value"} <= set(
        columns
    )
    assert columns["_is_current"].type is ColumnType.BOOLEAN
    assert columns["_valid_to"].not_null is False


def test_table_templates_describe_the_tables_of_an_entity() -> None:
    templates = load_table_templates()
    orders = load_entity("orders")

    assert set(templates) == {"bronze", "bronze_quarantine"}
    assert templates["bronze"].describe(orders).startswith(f"{orders.description}. Change log")
    assert "source table orders" in templates["bronze_quarantine"].describe(orders)
    assert templates["bronze"].metadata_columns[0] == load_metadata_columns()["_record_source"]


def test_a_template_fills_in_the_entity_name_and_description() -> None:
    template = TableTemplate(name="silver", description="{entity}: {entity_description}")
    entity = EntityDefinition(name="things", key="id", description="Things", columns=())

    assert template.describe(entity) == "things: Things"


def test_schemas_can_be_loaded_from_another_folder(schemas: SchemasTree) -> None:
    schemas.write_thing()
    schemas.write_silver()

    things = load_entity("things", schemas.root)
    silver = load_table_templates(schemas.root)["silver"]

    assert [column.lake_name for column in things.all_columns] == [
        "thing_id",
        "thing_name",
        *AUDIT_COLUMNS,
    ]
    assert "things" in [entity.name for entity in load_entities(schemas.root)]
    assert silver.describe(things) == "Versions of things: Things"
    assert [column.name for column in silver.metadata_columns] == [
        "_valid_from",
        "_valid_to",
        "_is_current",
        "_is_deleted",
    ]


def test_a_source_table_can_include_no_shared_columns(schemas: SchemasTree) -> None:
    schemas.write_thing(("include",), DELETE)

    assert load_entity("things", schemas.root).included_columns == ()


@pytest.mark.parametrize(
    ("path", "value", "problem"),
    [
        (("type",), "enum", 'the schema must be of "type": "record"'),
        (("name",), "stuff", "record stuff must be defined in stuff.avsc"),
        (("doc",), DELETE, "record things needs a non-blank doc"),
        (("owner",), "me", "unknown properties of record things: owner"),
        (("key",), "id", "key id is not a field of record things"),
        (("key",), DELETE, "record things needs a non-blank key"),
        (("include",), "audit", "include of record things must be a list of names"),
        (("fields",), {}, "fields of record things must be a list"),
        (("fields",), [], "record things has no fields"),
        (("fields", 0), "thing_id", "every field must be a JSON object"),
        (("fields", 0, "name"), DELETE, "a field needs a non-blank name"),
        (("fields", 0, "type"), "text", 'unsupported type "text" of field thing_id'),
        (("fields", 0, "type"), ["null", "long", "string"], "unsupported type"),
        (("fields", 0, "type"), ["long", "null"], "unsupported type"),
        (("fields", 0, "type"), {"type": "long"}, "unsupported type"),
        (("fields", 0, "doc"), " ", "field thing_id needs a non-blank doc"),
        (("fields", 0, "doc"), DELETE, "field thing_id needs a non-blank doc"),
        (("fields", 0, "order"), "ascending", "unknown properties of field thing_id: order"),
        (("fields", 0, "default"), 0, "field thing_id is not nullable and takes no default"),
        (("fields", 1, "default"), DELETE, 'nullable field thing_nmae needs "default": null'),
        (("fields", 1, "default"), "x", 'nullable field thing_nmae needs "default": null'),
        (("fields", 1, "renamed_to"), "", "field thing_nmae needs a non-blank renamed_to"),
        (("fields", 1, "renamed_to"), "thing_id", "two columns in the lake are named thing_id"),
        (("fields", 1, "renamed_to"), "created_by", "two columns in the lake are named created"),
        (("fields", 1, "name"), "thing_id", "two fields are named thing_id"),
    ],
)
def test_a_broken_source_schema_is_refused_naming_its_file(
    schemas: SchemasTree, path: Location, value: object, problem: str
) -> None:
    schemas.write_thing(path, value)

    with pytest.raises(DefinitionError, match=r"things\.avsc: ") as error:
        load_entity("things", schemas.root)

    assert problem in str(error.value)


@pytest.mark.parametrize(
    ("text", "problem"),
    [("{", "invalid JSON"), ("[]", "the schema must be a JSON object")],
)
def test_a_schema_file_that_is_not_a_json_object_is_refused(
    schemas: SchemasTree, text: str, problem: str
) -> None:
    schemas.write("sources/things.avsc", text)

    with pytest.raises(DefinitionError, match=problem):
        load_entity("things", schemas.root)


def test_an_include_that_names_no_shared_schema_is_refused(schemas: SchemasTree) -> None:
    schemas.write_thing(("include",), ["owners"])

    with pytest.raises(DefinitionError, match=r"owners\.avsc: no such schema file"):
        load_entity("things", schemas.root)


def test_every_entity_is_checked_when_all_are_loaded(schemas: SchemasTree) -> None:
    schemas.write_thing(("fields", 0, "type"), "text")

    with pytest.raises(DefinitionError, match="unsupported type"):
        load_entities(schemas.root)


def test_an_unknown_entity_has_no_schema_file(schemas: SchemasTree) -> None:
    with pytest.raises(DefinitionError, match=r"invoices\.avsc: no such schema file"):
        load_entity("invoices", schemas.root)


def test_a_broken_shared_schema_is_refused(schemas: SchemasTree) -> None:
    audit = json.loads((schemas.root / "audit.avsc").read_text())
    audit["fields"][0]["renamed_to"] = "modified_at"
    schemas.write("audit.avsc", audit)

    with pytest.raises(DefinitionError, match=r"audit\.avsc: unknown properties of field upd"):
        load_shared_columns("audit", schemas.root)


@pytest.mark.parametrize(
    ("path", "value", "problem"),
    [
        (("doc",), "{entity.name}", "unknown placeholders of record silver: entity.name"),
        (("doc",), "{table}", "unknown placeholders of record silver: table"),
        (("metadata",), "_valid_from", "metadata of record silver must be a list of names"),
        (("metadata",), ["_nope"], "metadata of record silver not in metadata.avsc: _nope"),
        (("metadata",), ["_is_current", "_is_current"], "two metadata columns are named _is_"),
        (("key",), "id", "unknown properties of record silver: key"),
        (
            ("fields",),
            [{"name": "x", "type": "long", "doc": "X"}],
            "record silver names its metadata columns in metadata, not fields",
        ),
    ],
)
def test_a_broken_table_schema_is_refused(
    schemas: SchemasTree, path: Location, value: object, problem: str
) -> None:
    schemas.write_silver(path, value)

    with pytest.raises(DefinitionError, match=r"silver\.avsc: ") as error:
        load_table_templates(schemas.root)

    assert problem in str(error.value)
