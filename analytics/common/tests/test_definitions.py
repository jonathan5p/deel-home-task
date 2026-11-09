import pytest

from analytics.common.tests.definitions_setup import AUDIT_COLUMNS, DefinitionsTree
from common.definitions import (
    ColumnDefinition,
    DefinitionError,
    EntityDefinition,
    LogicalType,
    TableTemplate,
    load_audit_columns,
    load_columns,
    load_entities,
    load_entity,
    load_table_templates,
)


def test_every_source_table_has_an_entity_ordered_by_name() -> None:
    assert [entity.name for entity in load_entities()] == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]


def test_an_entity_keeps_the_column_order_of_its_file_then_the_audit_columns() -> None:
    customers = load_entity("customers")

    assert customers.key == "customer_id"
    assert customers.description
    assert [column.name for column in customers.columns] == [
        "customer_id",
        "customer_name",
        "is_active",
        "customer_address",
    ]
    assert tuple(column.name for column in customers.audit_columns) == AUDIT_COLUMNS
    assert customers.all_columns == (*customers.columns, *customers.audit_columns)


def test_columns_carry_their_type_description_and_constraints() -> None:
    (unity_price,) = (c for c in load_entity("products").columns if c.name == "unity_price")

    assert unity_price == ColumnDefinition(
        name="unity_price",
        type=LogicalType.DECIMAL,
        description="Unit price of the product, decoded from Debezium's variable-scale decimal",
        not_null=True,
    )


def test_a_misspelled_source_column_is_renamed_in_the_lake() -> None:
    (quanity,) = (c for c in load_entity("order_items").columns if c.name == "quanity")

    assert quanity.renamed_to == "quantity"
    assert quanity.lake_name == "quantity"
    assert quanity.type is LogicalType.INTEGER


def test_a_column_that_is_not_renamed_keeps_its_name_in_the_lake() -> None:
    column = ColumnDefinition("status", LogicalType.VARCHAR, "Status")

    assert column.lake_name == "status"


def test_the_audit_columns_are_shared_by_every_entity() -> None:
    audit = load_audit_columns()

    assert tuple(column.name for column in audit) == AUDIT_COLUMNS
    assert [column.type for column in audit] == [
        LogicalType.TIMESTAMP,
        LogicalType.BIGINT,
        LogicalType.TIMESTAMP,
        LogicalType.BIGINT,
    ]
    assert all(entity.audit_columns == audit for entity in load_entities())


def test_pipeline_columns_are_described_by_name() -> None:
    columns = load_columns()

    assert columns["_commit_sequence"] == ColumnDefinition(
        name="_commit_sequence",
        type=LogicalType.BIGINT,
        description=(
            "Position of the change in the source's commit log (Debezium source.lsn);"
            " increases in commit order"
        ),
    )
    assert {"_valid_from", "_valid_to", "_is_current", "_is_deleted", "key", "value"} <= set(
        columns
    )
    assert columns["_is_current"].type is LogicalType.BOOLEAN


def test_table_templates_describe_the_tables_of_an_entity() -> None:
    templates = load_table_templates()
    orders = load_entity("orders")

    assert set(templates) == {"bronze", "bronze_quarantine"}
    assert templates["bronze"].describe(orders).startswith(f"{orders.description}. Change log")
    assert "source table orders" in templates["bronze_quarantine"].describe(orders)


def test_a_template_fills_in_the_entity_name_and_description() -> None:
    template = TableTemplate(name="silver", description="{entity}: {entity_description}")
    entity = EntityDefinition(name="things", key="id", description="Things", columns=())

    assert template.describe(entity) == "things: Things"


def test_definitions_can_be_loaded_from_another_folder(definitions: DefinitionsTree) -> None:
    definitions.write_thing()

    things = load_entity("things", definitions.root)

    assert [column.lake_name for column in things.all_columns] == [
        "thing_id",
        "thing_name",
        *AUDIT_COLUMNS,
    ]
    assert "things" in [entity.name for entity in load_entities(definitions.root)]


def test_an_entity_can_leave_out_the_audit_columns(definitions: DefinitionsTree) -> None:
    definitions.write_thing(replace=("audit_columns = true", "audit_columns = false"))

    assert load_entity("things", definitions.root).audit_columns == ()


@pytest.mark.parametrize(
    ("replace", "extra", "problem"),
    [
        (('type = "bigint"', 'type = "text"'), "", "unknown type 'text' of column thing_id"),
        (('description = "Thing identifier"', 'description = " "'), "", "non-blank description"),
        (('description = "Thing identifier"', ""), "", "column thing_id needs a non-blank desc"),
        (('description = "Things"', ""), "", "the entity needs a non-blank description"),
        (('key = "thing_id"', 'key = "id"'), "", "key id is not a column of entity things"),
        (("audit_columns", "owner = 1\naudit_columns"), "", "unknown fields of the entity: owner"),
        (("not_null = true", "nullable = false"), "", "unknown fields of column thing_id: nulla"),
        (("not_null = true", 'not_null = "yes"'), "", "not_null of column thing_id must be true"),
        (('"thing_name"', '"thing_id"'), "", "two columns are named thing_id in the lake"),
        (('renamed_to = "thing_name"', 'renamed_to = ""'), "", "non-blank renamed_to"),
        (('name = "things"', 'name = "stuff"'), "", "entity stuff must be defined in stuff.toml"),
        (("[columns.thing_id]", "[columns.thing_id"), "", "invalid TOML"),
        (("", ""), '\n[columns.updated_at]\ntype = "date"\ndescription = "x"\n', "two columns"),
        (("", ""), "\n[columns]\nbroken = 1\n", "column broken must be a table"),
    ],
)
def test_a_broken_entity_is_refused_naming_its_file(
    definitions: DefinitionsTree, replace: tuple[str, str], extra: str, problem: str
) -> None:
    definitions.write_thing(replace=replace, extra=extra)

    with pytest.raises(DefinitionError, match=r"things\.toml: ") as error:
        load_entity("things", definitions.root)

    assert problem in str(error.value)


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ('name = "things"\nkey = "id"\ndescription = "Things"\n', "columns must be a table"),
        ('name = "things"\nkey = "id"\ndescription = "Things"\ncolumns = {}\n', "has no columns"),
    ],
)
def test_an_entity_needs_columns(definitions: DefinitionsTree, text: str, problem: str) -> None:
    definitions.write_entity(text)

    with pytest.raises(DefinitionError, match=problem):
        load_entity("things", definitions.root)


def test_every_entity_is_checked_when_all_are_loaded(definitions: DefinitionsTree) -> None:
    definitions.write_thing(replace=('type = "bigint"', 'type = "text"'))

    with pytest.raises(DefinitionError, match="unknown type 'text'"):
        load_entities(definitions.root)


def test_an_unknown_entity_has_no_definition_file(definitions: DefinitionsTree) -> None:
    with pytest.raises(DefinitionError, match=r"invoices\.toml: no such definition file"):
        load_entity("invoices", definitions.root)


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ('[_x]\ntype = "bigint"\n', "column _x needs a non-blank description"),
        ('[_x]\ntype = "bigint"\ndescription = "X"\nnot_null = true\n', "unknown fields of col"),
        ("_x = 1\n", "column _x must be a table"),
    ],
)
def test_broken_pipeline_columns_are_refused(
    definitions: DefinitionsTree, text: str, problem: str
) -> None:
    definitions.write("columns.toml", text)

    with pytest.raises(DefinitionError, match=r"columns\.toml: ") as error:
        load_columns(definitions.root)

    assert problem in str(error.value)


def test_broken_audit_columns_are_refused(definitions: DefinitionsTree) -> None:
    definitions.write("audit.toml", '[updated_at]\ntype = "datetime"\ndescription = "x"\n')

    with pytest.raises(DefinitionError, match=r"audit\.toml: unknown type 'datetime'"):
        load_audit_columns(definitions.root)


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ('[silver]\ndescription = "{entity.name}"\n', "unknown placeholders of table silver"),
        ('[silver]\ndescription = "{table}"\n', "unknown placeholders of table silver: table"),
        ('[silver]\ndescription = ""\n', "table silver needs a non-blank description"),
        ('[silver]\ndescription = "x"\nowner = "y"\n', "unknown fields of table silver: owner"),
        ('silver = "x"\n', "silver must be a table"),
    ],
)
def test_broken_table_templates_are_refused(
    definitions: DefinitionsTree, text: str, problem: str
) -> None:
    definitions.write("tables.toml", text)

    with pytest.raises(DefinitionError, match=r"tables\.toml: ") as error:
        load_table_templates(definitions.root)

    assert problem in str(error.value)
