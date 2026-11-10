import json
from pathlib import Path

import pytest
from fastavro import parse_schema

from analytics.pipelines.tests.schemas.render_setup import (
    PRICE,
    SILVER,
    SILVER_ARCHIVE,
    THING_ID,
    THING_NAME,
    THINGS,
    schema_files,
    silver_schemas,
)
from pipelines.schemas.definitions import load_entities, load_table_templates
from pipelines.schemas.render import (
    avro_field,
    main,
    render_table_schema,
    render_tables,
    schema_text,
    write_schemas,
)

RENDERED = render_tables(load_table_templates().values(), load_entities())


def test_every_entity_has_a_rendered_bronze_quarantine_and_silver_schema() -> None:
    assert sorted(RENDERED) == [
        "bronze/customers.avsc",
        "bronze/customers_quarantine.avsc",
        "bronze/order_items.avsc",
        "bronze/order_items_quarantine.avsc",
        "bronze/orders.avsc",
        "bronze/orders_quarantine.avsc",
        "bronze/products.avsc",
        "bronze/products_quarantine.avsc",
        "silver/customers.avsc",
        "silver/order_items.avsc",
        "silver/orders.avsc",
        "silver/products.avsc",
    ]


@pytest.mark.parametrize("path", sorted(RENDERED))
def test_every_rendered_schema_is_valid_avro(path: str) -> None:
    parse_schema(json.loads(schema_text(RENDERED[path])))


def test_a_not_null_column_has_its_plain_avro_type() -> None:
    assert avro_field(THING_ID) == {"name": "thing_id", "type": "long", "doc": "Thing identifier"}
    assert avro_field(PRICE)["type"] == {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 38,
        "scale": 10,
    }


def test_a_nullable_column_is_a_union_with_null_under_its_lake_name() -> None:
    assert avro_field(THING_NAME) == {
        "name": "thing_name",
        "type": ["null", "string"],
        "default": None,
        "doc": "Name",
    }


def test_a_table_is_rendered_as_one_flat_record_in_its_layer_namespace() -> None:
    schema = render_table_schema(SILVER, THINGS)

    assert schema == {
        "type": "record",
        "name": "things",
        "namespace": "lake.silver",
        "doc": "Versions of things: Things",
        "fields": [avro_field(THING_ID), avro_field(THING_NAME), avro_field(PRICE)],
    }


def test_every_template_is_rendered_for_every_entity_by_layer_and_table() -> None:
    schemas = silver_schemas()

    assert sorted(schemas) == ["silver/things.avsc", "silver/things_archive.avsc"]
    assert schemas["silver/things_archive.avsc"] == render_table_schema(SILVER_ARCHIVE, THINGS)


def test_schema_text_puts_each_property_on_its_own_line_and_each_type_on_one() -> None:
    text = schema_text(render_table_schema(SILVER, THINGS))

    assert json.loads(text) == render_table_schema(SILVER, THINGS)
    assert '      "type": ["null", "string"],\n' in text
    assert '    {\n      "name": "thing_id",\n' in text
    assert text.endswith("  ]\n}\n")


def test_writing_the_schemas_replaces_stale_files_of_their_layers(tmp_path: Path) -> None:
    (tmp_path / "silver").mkdir()
    (tmp_path / "silver" / "gone.avsc").write_text("{}")
    (tmp_path / "gold").mkdir()
    (tmp_path / "gold" / "kept.avsc").write_text("{}")

    write_schemas(tmp_path, silver_schemas())
    write_schemas(tmp_path, silver_schemas())

    assert schema_files(tmp_path) == {
        "gold/kept.avsc": "{}",
        **{path: schema_text(schema) for path, schema in silver_schemas().items()},
    }


def test_the_command_writes_every_table_schema_to_the_given_folder(tmp_path: Path) -> None:
    main([str(tmp_path / "schemas")])

    assert schema_files(tmp_path / "schemas") == {
        path: schema_text(schema) for path, schema in RENDERED.items()
    }
