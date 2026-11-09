import argparse
import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

from pipelines.schemas.definitions import (
    AVRO_TYPES,
    SCHEMA_SUFFIX,
    ColumnDefinition,
    EntityDefinition,
    TableTemplate,
    load_entities,
    load_table_templates,
)

LAKE_NAMESPACE = "lake"

type Schema = dict[str, object]


def avro_field(column: ColumnDefinition) -> Schema:
    """The column as a plain Avro field, under its lake name."""
    avro_type = AVRO_TYPES[column.type]
    if column.not_null:
        return {"name": column.lake_name, "type": avro_type, "doc": column.description}
    return {
        "name": column.lake_name,
        "type": ["null", avro_type],
        "default": None,
        "doc": column.description,
    }


def render_table_schema(template: TableTemplate, entity: EntityDefinition) -> Schema:
    """The entity's table in the template's layer, as one flat, standard Avro record."""
    return {
        "type": "record",
        "name": template.table_name(entity),
        "namespace": f"{LAKE_NAMESPACE}.{template.layer}",
        "doc": template.describe(entity),
        "fields": [avro_field(column) for column in template.columns_for(entity)],
    }


def render_tables(
    templates: Iterable[TableTemplate], entities: Iterable[EntityDefinition]
) -> dict[str, Schema]:
    """Every template rendered for every entity, by path: `<layer>/<table>.avsc`."""
    entities = tuple(entities)
    return {
        f"{template.layer}/{template.table_name(entity)}{SCHEMA_SUFFIX}": render_table_schema(
            template, entity
        )
        for template in templates
        for entity in entities
    }


def _object_text(value: Mapping[str, object], depth: int) -> str:
    pad = "  " * depth
    lines = []
    for key, item in value.items():
        if isinstance(item, list) and item and all(isinstance(entry, dict) for entry in item):
            entries = ",\n".join(f"{pad}    {_object_text(entry, depth + 2)}" for entry in item)
            text = f"[\n{entries}\n{pad}  ]"
        else:
            text = json.dumps(item, ensure_ascii=False)
        lines.append(f"{pad}  {json.dumps(key)}: {text}")
    return "{\n" + ",\n".join(lines) + f"\n{pad}}}"


def schema_text(schema: Schema) -> str:
    """The schema as JSON with one property per line, but each type on one line, like the
    hand-written schemas."""
    return _object_text(schema, 0) + "\n"


def write_schemas(out_dir: Path, schemas: Mapping[str, Schema]) -> None:
    """Write each schema under `out_dir` and remove the stale ones of the same layers."""
    for relative, schema in schemas.items():
        path = out_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(schema_text(schema), encoding="utf-8")
    for layer in {Path(relative).parent for relative in schemas}:
        for path in (out_dir / layer).glob(f"*{SCHEMA_SUFFIX}"):
            if path.relative_to(out_dir).as_posix() not in schemas:
                path.unlink()


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m pipelines.schemas.render",
        description="Write every lake table as a flat Avro schema: <out_dir>/<layer>/<table>.avsc",
    )
    parser.add_argument("out_dir", type=Path)
    out_dir: Path = parser.parse_args(argv).out_dir
    write_schemas(out_dir, render_tables(load_table_templates().values(), load_entities()))


if __name__ == "__main__":
    main(sys.argv[1:])
