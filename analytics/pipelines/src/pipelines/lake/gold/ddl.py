import argparse
import sys
from collections.abc import Iterable, Mapping, Sequence
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

from jinja2 import Environment, StrictUndefined

from pipelines.lake.gold.dimensions import change_facts, scd2_dimensions
from pipelines.lake.gold.public import (
    PUBLIC_SCHEMA,
    PUBLIC_VIEWS,
    PublicView,
    UnknownColumnError,
    public_columns,
    view_query,
)
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, postgres_string
from pipelines.lake.gold.stored import StoredTable, stored_tables
from pipelines.schemas.definitions import (
    POSTGRES_TYPES,
    SCHEMAS,
    ColumnDefinition,
    load_entities,
)
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

TEMPLATE: Traversable = files("pipelines.lake.gold") / "sql" / "ddl.sql.j2"
SYNC_STATE = "sync_state"


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


def describe_view(table: GoldTable, hidden: Iterable[str] = ()) -> str:
    """The comments of the public view of `table`."""
    relation = f"{PUBLIC_SCHEMA}.{table.name}"
    return comments("VIEW", relation, table.view_description, public_columns(table, hidden))


def view_statement(view: PublicView, tables: Mapping[str, GoldTable]) -> str:
    """The view, from the query the lake's gold view renders too, and its comments."""
    query = view_query(view, tables, INTERNAL_SCHEMA)
    return (
        f"CREATE OR REPLACE VIEW {PUBLIC_SCHEMA}.{view.name} AS\n{query};\n\n"
        f"{describe_view(tables[view.name], view.hidden)}"
    )


def table_statement(stored: StoredTable) -> str:
    """The table, and for a history table its default partition, with their comments.

    The key is the primary key, and the table's `unique` columns are unique too.
    """
    key = ", ".join(stored.postgres_key)
    lines = f"{column_lines(stored.table)}\n    PRIMARY KEY ({key})"
    if stored.unique:
        lines += f",\n    UNIQUE ({', '.join(stored.unique)})"
    partitioned = " PARTITION BY RANGE (_changed_at)" if stored.history else ""
    statement = (
        f"CREATE TABLE IF NOT EXISTS {stored.relation} (\n{lines}\n){partitioned};\n\n"
        f"{describe_table(stored.table)}"
    )
    if not stored.history:
        return statement
    default = f"{stored.relation}_default"
    return (
        f"{statement}\n\nCREATE TABLE IF NOT EXISTS {default}\n"
        f"    PARTITION OF {stored.relation} DEFAULT;\n\n"
        f"COMMENT ON TABLE {default} IS\n"
        f"    {postgres_string(f'Rows of {stored.relation} outside every monthly partition.')};"
    )


def staging_statement(stored: StoredTable) -> str:
    return (
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {stored.staging_relation} (\n"
        f"{column_list(stored.staging_table)}\n);\n\n{describe_table(stored.staging_table)}"
    )


def render(root: Traversable = SCHEMAS, template: Traversable = TEMPLATE) -> str:
    environment = Environment(
        autoescape=False,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tables = load_gold_tables(root)
    entities = load_entities(root)
    dimensions = scd2_dimensions(tables, entities)
    facts = change_facts(tables, entities)
    stored = stored_tables(tables, dimensions.values(), facts.values())
    return environment.from_string(template.read_text(encoding="utf-8")).render(
        tables=tables,
        stored=stored.values(),
        history=[table for table in stored.values() if table.history],
        sync_state=tables[SYNC_STATE],
        views=[view_statement(view, tables) for view in PUBLIC_VIEWS],
        table_statement=table_statement,
        staging_statement=staging_statement,
        column_lines=column_lines,
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
