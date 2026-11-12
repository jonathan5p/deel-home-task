from collections.abc import Mapping
from itertools import takewhile
from typing import NamedTuple, Self

from pyspark.sql import SparkSession

from pipelines.lake.shared.tables import TABLE_TEMPLATES, sql_identifier, sql_string
from pipelines.schemas.definitions import ColumnDefinition, EntityDefinition

QUERY_START = "\nAS\n"


class ViewDefinition(NamedTuple):
    """A lake view's description, its columns by lake name, and the query that computes them."""

    comment: str
    columns: Mapping[str, ColumnDefinition]
    query: str


class StoredView(NamedTuple):
    """What a view stores that its definition decides: the query, the comment and the column
    comments, by column name."""

    query: str
    comment: str | None
    columns: Mapping[str, str | None]

    @classmethod
    def of(cls, definition: ViewDefinition) -> Self:
        columns = {name: column.description for name, column in definition.columns.items()}
        return cls(query=definition.query, comment=definition.comment, columns=columns)


def view_definition(template_name: str, entity: EntityDefinition, query: str) -> ViewDefinition:
    template = TABLE_TEMPLATES[template_name]
    columns = template.columns_for(entity)
    return ViewDefinition(
        comment=template.describe(entity),
        columns={column.lake_name: column for column in columns},
        query=query,
    )


def create_view_statement(view: str, definition: ViewDefinition) -> str:
    columns = ",\n".join(
        f"  {sql_identifier(name)} COMMENT {sql_string(column.description)}"
        for name, column in definition.columns.items()
    )
    return (
        f"CREATE OR REPLACE VIEW {view} (\n{columns})\n"
        f"COMMENT {sql_string(definition.comment)}{QUERY_START}{definition.query}"
    )


def stored_view(spark: SparkSession, view: str) -> StoredView | None:
    """The query and descriptions `view` stores, or None when the catalog has no such view."""
    namespace, _, name = view.rpartition(".")
    views = spark.sql(f"SHOW VIEWS IN {namespace}").collect()
    if not any(row.viewName == name and not row.isTemporary for row in views):
        return None
    details = spark.sql(f"DESCRIBE TABLE EXTENDED {view}").collect()
    columns = {row.col_name: row.comment for row in takewhile(lambda row: row.col_name, details)}
    comment = next((row.data_type for row in details if row.col_name == "Comment"), None)
    (ddl,) = spark.sql(f"SHOW CREATE TABLE {view}").collect()
    query = str(ddl[0]).partition(QUERY_START)[2].removesuffix("\n")
    return StoredView(query=query, comment=comment, columns=columns)


def ensure_view(spark: SparkSession, view: str, definition: ViewDefinition) -> str | None:
    """Create `view`, or replace it when its stored query or descriptions differ.

    Returns the statement that created or replaced it, or None when it was up to date, so an
    unchanged view keeps its version.
    """
    if stored_view(spark, view) == StoredView.of(definition):
        return None
    statement = create_view_statement(view, definition)
    spark.sql(statement)
    return statement
