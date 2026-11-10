from collections.abc import Iterable, Mapping

from pipelines.bronze.entities import Entity
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.lake.tables import sql_identifier as q
from pipelines.lake.views import ViewDefinition, view_definition
from pipelines.settings import SilverSettings

HISTORY = "silver_history"
CURRENT = "silver_current"
FOLDED = "_folded"


def _columns(template: str, entity: Entity) -> list[str]:
    return [column.lake_name for column in TABLE_TEMPLATES[template].columns_for(entity.definition)]


def _select(expressions: Iterable[str], indent: str = "    ") -> str:
    return ",\n".join(f"{indent}{expression}" for expression in expressions)


def history_query(entity: Entity, table: str) -> str:
    content_hash = q(MetadataColumn.CONTENT_HASH)
    is_deleted = q(MetadataColumn.IS_DELETED)
    changed_at = q(MetadataColumn.CHANGED_AT)
    commit_sequence = q(MetadataColumn.COMMIT_SEQUENCE)
    by_key = f"PARTITION BY {q(entity.key_column)} ORDER BY {commit_sequence}"
    # The first version of a key has no previous one: lag() is null and coalesce keeps it.
    folded = (
        f"coalesce({content_hash} = lag({content_hash}) OVER ({by_key})"
        f" AND {is_deleted} = lag({is_deleted}) OVER ({by_key}), false) AS {q(FOLDED)}"
    )
    derived: dict[str, str] = {
        MetadataColumn.VALID_FROM: changed_at,
        MetadataColumn.VALID_TO: f"lead({changed_at}) OVER ({by_key})",
        MetadataColumn.IS_CURRENT: f"lead({commit_sequence}) OVER ({by_key}) IS NULL",
    }
    versions = [*(q(name) for name in _columns("silver", entity)), folded]
    history = [
        f"{derived[name]} AS {q(name)}" if name in derived else q(name)
        for name in _columns(HISTORY, entity)
    ]
    # WHERE runs before the outer window functions, so a version stays valid until the next
    # version that is not folded.
    return (
        "WITH versions AS (\n"
        "    SELECT\n"
        f"{_select(versions, indent='        ')}\n"
        f"    FROM {table}\n"
        ")\n"
        "SELECT\n"
        f"{_select(history)}\n"
        "FROM versions\n"
        f"WHERE NOT {q(FOLDED)}"
    )


def current_query(entity: Entity, history: str) -> str:
    return (
        "SELECT\n"
        f"{_select(q(name) for name in _columns(CURRENT, entity))}\n"
        f"FROM {history}\n"
        f"WHERE {q(MetadataColumn.IS_CURRENT)} AND NOT {q(MetadataColumn.IS_DELETED)}"
    )


def silver_views(entity: Entity, settings: SilverSettings) -> Mapping[str, ViewDefinition]:
    # The history view comes first: the current view reads it.
    history = settings.table(TABLE_TEMPLATES[HISTORY].table_name(entity.definition))
    current = settings.table(TABLE_TEMPLATES[CURRENT].table_name(entity.definition))
    table = settings.table(entity.name)
    return {
        history: view_definition(HISTORY, entity.definition, history_query(entity, table)),
        current: view_definition(CURRENT, entity.definition, current_query(entity, history)),
    }
