from collections.abc import Iterable, Mapping
from importlib.resources import files

from jinja2 import Environment, StrictUndefined
from pyspark.sql import SparkSession

from pipelines.lake.bronze.entities import Entity, entity_for_topic
from pipelines.lake.shared.tables import TABLE_TEMPLATES
from pipelines.lake.shared.views import ViewDefinition, ensure_view, view_definition
from pipelines.lake.silver.job import SILVER
from pipelines.settings import SilverSettings

HISTORY = "silver_history"
CURRENT = "silver_current"
SQL = Environment(undefined=StrictUndefined, autoescape=False)


class MissingSilverTablesError(LookupError):
    def __init__(self, tables: Iterable[str]) -> None:
        super().__init__(
            f"The silver views read tables that do not exist yet: {', '.join(tables)}."
            " Start the silver job first."
        )


def _render(template: str, **values: object) -> str:
    text = (files("pipelines.lake.silver") / f"{template}.sql.j2").read_text(encoding="utf-8")
    return SQL.from_string(text).render(**values)


def _columns(template: str, entity: Entity) -> list[str]:
    return [column.lake_name for column in TABLE_TEMPLATES[template].columns_for(entity.definition)]


def history_query(entity: Entity, table: str) -> str:
    return _render("history", table=table, key=entity.lake_key, columns=_columns(SILVER, entity))


def current_query(entity: Entity, history: str) -> str:
    return _render("current", history=history, columns=_columns(CURRENT, entity))


def silver_views(entity: Entity, settings: SilverSettings) -> Mapping[str, ViewDefinition]:
    """The entity's history view, then its current view, which reads the history view."""
    history = settings.table(TABLE_TEMPLATES[HISTORY].table_name(entity.definition))
    current = settings.table(TABLE_TEMPLATES[CURRENT].table_name(entity.definition))
    table = settings.table(entity.name)
    return {
        history: view_definition(HISTORY, entity.definition, history_query(entity, table)),
        current: view_definition(CURRENT, entity.definition, current_query(entity, history)),
    }


def refresh_views(spark: SparkSession, settings: SilverSettings) -> list[str]:
    """Create the silver views of every entity, or replace those whose query or descriptions
    changed; returns the statements that ran, so an empty list means all were up to date.

    Fails before touching any view while a silver table is missing.
    """
    entities = [entity_for_topic(topic) for topic in settings.topics]
    missing = [
        table
        for table in (settings.table(entity.name) for entity in entities)
        if not spark.catalog.tableExists(table)
    ]
    if missing:
        raise MissingSilverTablesError(missing)
    statements = []
    for entity in entities:
        for view, definition in silver_views(entity, settings).items():
            statement = ensure_view(spark, view, definition)
            if statement is not None:
                statements.append(statement)
    return statements
