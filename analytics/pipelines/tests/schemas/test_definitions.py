import json
from pathlib import Path

import pytest
from fastavro import parse_schema

from analytics.pipelines.tests.schemas.definitions_setup import (
    AUDIT_COLUMNS,
    DELETE,
    SILVER,
    Location,
    SchemasTree,
    column_definitions,
    edited,
    schema_files,
)
from pipelines.schemas.definitions import (
    AVRO_TYPES,
    SCHEMAS,
    ColumnDefinition,
    ColumnGroup,
    ColumnType,
    DefinitionError,
    EntityDefinition,
    TableTemplate,
    load_entities,
    load_entity,
    load_shared_columns,
    load_table_templates,
)

SHIPPED = schema_files(Path(str(SCHEMAS)))


def test_the_schemas_folder_holds_the_shared_sources_tables_and_gold_schemas() -> None:
    root = Path(str(SCHEMAS))

    assert [str(file.relative_to(root)) for file in SHIPPED] == [
        "audit.avsc",
        "gold/current_order_items.avsc",
        "gold/current_orders.avsc",
        "gold/data_freshness.avsc",
        "gold/dim_customer.avsc",
        "gold/dim_date.avsc",
        "gold/dim_order_status.avsc",
        "gold/dim_product.avsc",
        "gold/fact_order_history.avsc",
        "gold/fact_order_item_history.avsc",
        "sources/customers.avsc",
        "sources/order_items.avsc",
        "sources/orders.avsc",
        "sources/products.avsc",
        "tables/bronze_quarantine_template.avsc",
        "tables/bronze_template.avsc",
        "tables/silver_current_template.avsc",
        "tables/silver_history_template.avsc",
        "tables/silver_template.avsc",
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


def test_a_template_defines_its_own_columns_in_its_fields() -> None:
    fields = {column.name: column for column in load_table_templates()["bronze"].fields}

    assert fields["_commit_sequence"] == ColumnDefinition(
        name="_commit_sequence",
        type=ColumnType.LONG,
        description=(
            "Position of the change in the source's commit log (Debezium source.lsn);"
            " increases in commit order"
        ),
        not_null=True,
    )
    assert fields["_published_at"].type is ColumnType.TIMESTAMP_MILLIS


def test_a_column_defined_in_several_schemas_is_defined_the_same_in_each() -> None:
    definitions = column_definitions(Path(str(SCHEMAS)))

    differing = {name: files for name, files in definitions.items() if len(files) > 1}

    assert differing == {}
    assert list(definitions["_record_source"].values()) == [
        ["tables/bronze_quarantine_template.avsc", "tables/bronze_template.avsc"]
    ]


def test_differing_definitions_of_a_column_are_found_in_any_folder(schemas: SchemasTree) -> None:
    schemas.write_silver(("fields", 1, "doc"), "Something else")
    (schemas.root / "example_layer").mkdir()
    schemas.write("example_layer/facts.avsc", edited(SILVER, ("fields", 1, "type"), "long"))

    definitions = column_definitions(schemas.root)["_is_current"]

    def definition_in(file: str) -> str:
        return next(definition for definition, files in definitions.items() if file in files)

    assert definition_in("example_layer/facts.avsc") != definition_in("tables/silver_template.avsc")


def test_business_columns_may_differ_between_schemas(schemas: SchemasTree) -> None:
    (schemas.root / "example_layer").mkdir()
    thing = {
        "type": "record",
        "name": "facts",
        "fields": [
            {"name": "customer_id", "type": ["null", "long"], "default": None, "doc": "A reference"}
        ],
    }
    schemas.write("example_layer/facts.avsc", json.dumps(thing))

    assert "customer_id" not in column_definitions(schemas.root)


def test_table_templates_describe_the_tables_of_an_entity() -> None:
    templates = load_table_templates()
    orders = load_entity("orders")

    assert set(templates) == {
        "bronze",
        "bronze_quarantine",
        "silver",
        "silver_history",
        "silver_current",
    }
    assert templates["bronze"].describe(orders).startswith(f"{orders.description}. Change log")
    assert "source table orders" in templates["bronze_quarantine"].describe(orders)


def test_a_template_is_partitioned_by_one_of_its_own_timestamp_fields(
    schemas: SchemasTree,
) -> None:
    schemas.write_silver(("partitioned_by",), "_valid_to")

    assert load_table_templates(schemas.root)["silver"].partitioned_by == "_valid_to"


def test_a_template_without_a_partition_column_is_unpartitioned(schemas: SchemasTree) -> None:
    schemas.write_silver()

    assert load_table_templates(schemas.root)["silver"].partitioned_by is None


def test_a_bronze_table_is_the_source_then_the_audit_then_the_metadata_columns() -> None:
    bronze = load_table_templates()["bronze"]
    order_items = load_entity("order_items")

    columns = bronze.columns_for(order_items)

    assert columns[: len(order_items.columns)] == order_items.columns
    assert columns[len(order_items.columns) :][: len(AUDIT_COLUMNS)] == (
        order_items.included_columns
    )
    assert columns[-len(bronze.fields) :] == bronze.fields
    assert [column.lake_name for column in columns][:5] == [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "updated_at",
    ]


def test_a_quarantine_table_holds_its_own_fields_only() -> None:
    quarantine = load_table_templates()["bronze_quarantine"]

    columns = quarantine.columns_for(load_entity("orders"))

    assert quarantine.include == ()
    assert columns == quarantine.fields
    assert [column.name for column in columns][-3:] == [
        "value",
        "_failure_reason",
        "_quarantined_at",
    ]


def test_a_template_names_the_table_of_each_entity_in_its_layer() -> None:
    templates = load_table_templates()
    orders = load_entity("orders")

    assert (templates["bronze"].layer, templates["bronze"].table_name(orders)) == (
        "bronze",
        "orders",
    )
    assert (
        templates["bronze_quarantine"].layer,
        templates["bronze_quarantine"].table_name(orders),
    ) == ("bronze", "orders_quarantine")
    assert [
        (templates[name].layer, templates[name].table_name(orders))
        for name in ("silver_history", "silver_current")
    ] == [("silver", "orders_history"), ("silver", "orders_current")]


def test_a_template_fills_in_the_entity_name_and_description() -> None:
    template = TableTemplate(name="silver", description="{entity}: {entity_description}")
    entity = EntityDefinition(name="things", key="id", description="Things", columns=())

    assert template.describe(entity) == "things: Things"


def test_a_shared_group_is_empty_for_an_entity_that_does_not_include_it() -> None:
    audit = load_shared_columns("audit")
    status = ColumnDefinition("status", ColumnType.STRING, "Status")
    entity = EntityDefinition(name="things", key="status", description="Things", columns=(status,))
    template = TableTemplate(
        name="silver",
        description="{entity}",
        include=(ColumnGroup("source"), ColumnGroup("audit", audit)),
    )

    assert template.columns_for(entity) == (status,)


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
    assert [column.lake_name for column in silver.columns_for(things)] == [
        "thing_id",
        "thing_name",
        *AUDIT_COLUMNS,
        "_valid_to",
        "_is_current",
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
        (("doc",), "{entity.name}", "unknown placeholders of record silver_template: entity.name"),
        (("doc",), "{table}", "unknown placeholders of record silver_template: table"),
        (("include",), "source", "include of record silver_template must be a list of names"),
        (("include",), ["things"], "unknown include of record silver_template: things; use source"),
        (("include",), ["source", "source"], "two includes are named source"),
        (("columns",), ["source"], "unknown properties of record silver_template: columns"),
        (("key",), "id", "unknown properties of record silver_template: key"),
        (("fields", 0, "renamed_to"), "x", "unknown properties of field _valid_to: renamed_to"),
        (("fields", 0, "default"), DELETE, 'nullable field _valid_to needs "default": null'),
        (("fields", 1, "name"), "_valid_to", "two fields are named _valid_to"),
        (("partitioned_by",), 3, "record silver_template needs a non-blank partitioned_by"),
        (
            ("partitioned_by",),
            "_changed_at",
            "partitioned_by of record silver_template must name one of its own timestamp or date"
            " fields, not _changed_at",
        ),
        (("partitioned_by",), "_is_current", "own timestamp or date fields, not _is_current"),
    ],
)
def test_a_broken_table_template_is_refused(
    schemas: SchemasTree, path: Location, value: object, problem: str
) -> None:
    schemas.write_silver(path, value)

    with pytest.raises(DefinitionError, match=r"silver_template\.avsc: ") as error:
        load_table_templates(schemas.root)

    assert problem in str(error.value)


def test_a_table_template_without_columns_is_refused(schemas: SchemasTree) -> None:
    schemas.write("tables/silver_template.avsc", {**SILVER, "include": [], "fields": []})

    with pytest.raises(DefinitionError, match=r"record silver_template has no columns"):
        load_table_templates(schemas.root)


def test_a_table_template_may_include_no_groups(schemas: SchemasTree) -> None:
    schemas.write_silver(("include",), DELETE)

    silver = load_table_templates(schemas.root)["silver"]

    assert silver.columns_for(load_entity("orders")) == silver.fields


def test_a_table_template_must_be_named_as_a_template(schemas: SchemasTree) -> None:
    schemas.write("tables/silver.avsc", {**SILVER, "name": "silver"})

    with pytest.raises(
        DefinitionError, match=r"silver\.avsc: record silver must be named <table>_te"
    ):
        load_table_templates(schemas.root)
