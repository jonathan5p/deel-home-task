from collections.abc import Iterable
from functools import cache
from importlib.resources import files

from jinja2 import Environment, StrictUndefined, Template


def qualify(columns: Iterable[str], relation: str) -> list[str]:
    return [f"{relation}.{column}" for column in columns]


SQL = Environment(autoescape=False, undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)
SQL.filters["qualify"] = qualify


@cache
def _template(name: str) -> Template:
    return SQL.from_string((files("pipelines.gold") / f"{name}.sql.j2").read_text(encoding="utf-8"))


def render_sql(name: str, **values: object) -> str:
    """Render the statement in the template `<name>.sql.j2` next to this module."""
    return _template(name).render(**values)
