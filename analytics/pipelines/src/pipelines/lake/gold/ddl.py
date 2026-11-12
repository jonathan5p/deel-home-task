import argparse
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

from jinja2 import Environment, StrictUndefined

from pipelines.lake.gold.dimensions import change_facts, scd2_dimensions
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, postgres_string
from pipelines.lake.gold.stored import StoredTable, WriteMode, stored_tables
from pipelines.schemas.definitions import (
    POSTGRES_TYPES,
    SCHEMAS,
    ColumnDefinition,
    load_entities,
)
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

TEMPLATE: Traversable = files("pipelines.lake.gold") / "sql" / "ddl.sql.j2"
PUBLIC_SCHEMA = "gold"
PIPELINE_ONLY_COLUMNS = frozenset(
    {"_commit_sequence", "_content_hash", "_is_deleted", "_is_folded", "_version_number"}
)
SYNC_STATE = "sync_state"
NOT_FOLDED = "NOT _is_folded"
LIVE_VERSIONS = "NOT _is_folded AND NOT _is_deleted"
NOT_DELETED = "NOT _is_deleted"


@dataclass(frozen=True, slots=True)
class PublicView:
    """A view in gold over one gold_internal table, by default the one of the same name: its
    columns but the pipeline-only and `hidden` ones, filtered by `condition`."""

    name: str
    condition: str | None = None
    hidden: tuple[str, ...] = ()
    table: str | None = None

    @property
    def source(self) -> str:
        return self.table or self.name


PUBLIC_VIEWS = (
    PublicView("dim_customer", LIVE_VERSIONS),
    PublicView("dim_product", LIVE_VERSIONS),
    PublicView("dim_date"),
    PublicView("dim_order_status"),
    PublicView("fact_order_history", NOT_FOLDED),
    PublicView("fact_order_item_history", NOT_FOLDED),
    PublicView("current_orders", NOT_DELETED, hidden=("is_open",)),
    PublicView("open_orders", "is_open AND NOT _is_deleted", table="current_orders"),
    PublicView("current_order_items", NOT_DELETED),
    PublicView("open_orders_by_delivery_date_and_status"),
    PublicView("top_delivery_dates_by_open_orders"),
    PublicView("pending_items_by_product"),
    PublicView("top_customers_by_pending_orders"),
)


class UnknownColumnError(ValueError):
    def __init__(self, table: str, columns: Iterable[str]) -> None:
        super().__init__(f"{table} has no columns named {', '.join(columns)}")


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


def public_columns(table: GoldTable, hidden: Iterable[str] = ()) -> list[ColumnDefinition]:
    """The columns of the public view of `table`: all but the pipeline-only ones and the
    `hidden` ones it keeps internal."""
    internal = set(hidden)
    unknown = sorted(internal - {column.name for column in table.columns})
    if unknown:
        raise UnknownColumnError(table.name, unknown)
    left_out = PIPELINE_ONLY_COLUMNS | internal
    return [column for column in table.columns if column.name not in left_out]


def describe_view(table: GoldTable, hidden: Iterable[str] = ()) -> str:
    """The comments of the public view of `table`."""
    relation = f"{PUBLIC_SCHEMA}.{table.name}"
    return comments("VIEW", relation, table.view_description, public_columns(table, hidden))


def view_statement(view: PublicView, tables: Mapping[str, GoldTable]) -> str:
    """The view and its comments."""
    table = tables[view.name]
    columns = ",\n".join(f"    {column.name}" for column in public_columns(table, view.hidden))
    condition = f"\nWHERE {view.condition}" if view.condition else ""
    return (
        f"CREATE OR REPLACE VIEW {PUBLIC_SCHEMA}.{view.name} AS\nSELECT\n{columns}\n"
        f"FROM {INTERNAL_SCHEMA}.{view.source}{condition};\n\n{describe_view(table, view.hidden)}"
    )


def table_statement(stored: StoredTable) -> str:
    """The table, and for a history table its default partition, with their comments.

    The key is the primary key, or for a recounted table a unique key whose nulls match, as
    the null-safe merge of its rows does; a replaced table has none. The table's `unique`
    columns are unique too.
    """
    key = ", ".join(stored.postgres_key)
    if stored.mode is WriteMode.RECOUNT:
        lines = f"{column_lines(stored.table)}\n    UNIQUE NULLS NOT DISTINCT ({key})"
    elif stored.keys:
        lines = f"{column_lines(stored.table)}\n    PRIMARY KEY ({key})"
        if stored.unique:
            lines += f",\n    UNIQUE ({', '.join(stored.unique)})"
    else:
        lines = column_list(stored.table)
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
    facts = change_facts(tables, entities, dimensions)
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
