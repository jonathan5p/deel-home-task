import argparse
import sys
from collections.abc import Iterable, Mapping, Sequence
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

from jinja2 import Environment, StrictUndefined

from pipelines.gold.dimensions import scd2_dimensions
from pipelines.gold.scd2 import INTERNAL_SCHEMA
from pipelines.schemas.definitions import (
    POSTGRES_TYPES,
    SCHEMAS,
    ColumnDefinition,
    load_entities,
)
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

TEMPLATE: Traversable = files("pipelines.gold") / "ddl.sql.j2"
PUBLIC_SCHEMA = "gold"
PIPELINE_ONLY_COLUMNS = frozenset(
    {"_commit_sequence", "_content_hash", "_is_deleted", "_is_folded"}
)


class UnknownColumnError(ValueError):
    def __init__(self, table: str, columns: Iterable[str]) -> None:
        super().__init__(f"{table} has no columns named {', '.join(columns)}")


def postgres_string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _column_definitions(table: GoldTable, constraints: Mapping[str, str]) -> list[str]:
    unknown = sorted(set(constraints) - {column.name for column in table.columns})
    if unknown:
        raise UnknownColumnError(table.name, unknown)
    definitions = []
    for column in table.columns:
        parts = [column.name, POSTGRES_TYPES[column.type]]
        if column.not_null:
            parts.append("NOT NULL")
        if column.name in constraints:
            parts.append(constraints[column.name])
        definitions.append(" ".join(parts))
    return definitions


def column_lines(table: GoldTable, constraints: Mapping[str, str] | None = None) -> str:
    """The column lines of a CREATE TABLE, each ending with a comma for the table constraints.

    `constraints` adds column constraints by column name, such as an identity or a reference.
    """
    return "\n".join(f"    {line}," for line in _column_definitions(table, constraints or {}))


def column_list(table: GoldTable) -> str:
    """The column lines of a CREATE TABLE without table constraints."""
    return ",\n".join(f"    {line}" for line in _column_definitions(table, {}))


def comments(
    kind: str, relation: str, description: str, columns: Iterable[ColumnDefinition]
) -> str:
    statements = [f"COMMENT ON {kind} {relation} IS\n    {postgres_string(description)};"]
    statements += [
        f"COMMENT ON COLUMN {relation}.{column.name} IS\n    {postgres_string(column.description)};"
        for column in columns
    ]
    return "\n".join(statements)


def describe_table(table: GoldTable, kind: str = "TABLE") -> str:
    """The comments of a table in gold_internal, or of a view there with `kind` VIEW."""
    return comments(kind, f"{INTERNAL_SCHEMA}.{table.name}", table.description, table.columns)


def describe_view(table: GoldTable, *added: ColumnDefinition) -> str:
    """The comments of the public view of `table`: its columns without the pipeline-only ones."""
    public = [column for column in table.columns if column.name not in PIPELINE_ONLY_COLUMNS]
    relation = f"{PUBLIC_SCHEMA}.{table.name}"
    return comments("VIEW", relation, table.view_description, [*public, *added])


def render(root: Traversable = SCHEMAS, template: Traversable = TEMPLATE) -> str:
    environment = Environment(
        autoescape=False,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tables = load_gold_tables(root)
    return environment.from_string(template.read_text(encoding="utf-8")).render(
        tables=tables,
        dimensions=scd2_dimensions(tables, load_entities(root)),
        column_lines=column_lines,
        column_list=column_list,
        describe_table=describe_table,
        describe_view=describe_view,
    )


def output_path(argv: Sequence[str]) -> Path:
    parser = argparse.ArgumentParser(prog="gold-ddl", description="Render the gold DDL.")
    parser.add_argument("output", type=Path, help="file to write the rendered SQL to")
    return parser.parse_args(argv).output


def main(argv: Sequence[str]) -> None:
    output_path(argv).write_text(render(), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1:])
