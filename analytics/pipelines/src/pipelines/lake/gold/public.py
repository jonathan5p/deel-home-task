from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable

from jinja2 import Environment, StrictUndefined

from pipelines.schemas.definitions import ColumnDefinition
from pipelines.schemas.gold_definitions import GoldTable

TEMPLATES: Traversable = files("pipelines.lake.gold") / "sql" / "views"
PROJECTION = "projection"
PUBLIC_SCHEMA = "gold"
PIPELINE_ONLY_COLUMNS = frozenset(
    {"_commit_sequence", "_content_hash", "_is_deleted", "_is_folded", "_version_number"}
)
FRESHNESS = "data_freshness"
LIVE_VERSIONS = "NOT _is_folded AND NOT _is_deleted"
NOT_DELETED = "NOT _is_deleted"
SQL = Environment(
    undefined=StrictUndefined,
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=False,
)


@dataclass(frozen=True, slots=True)
class PublicView:
    """A view in gold over the gold_internal tables. By default it projects the table of the
    same name, or `table`, to its public columns, filtered by `condition`; with `query`, its
    query is the template `sql/views/<name>.sql.j2`.

    PostgreSQL and the lake render the same templates, written in the SQL both accept, with
    only the namespace of the gold_internal tables as a parameter.
    """

    name: str
    condition: str | None = None
    hidden: tuple[str, ...] = ()
    table: str | None = None
    query: bool = False

    @property
    def source(self) -> str:
        return self.table or self.name


PUBLIC_VIEWS = (
    PublicView("dim_customer", LIVE_VERSIONS),
    PublicView("dim_product", LIVE_VERSIONS),
    PublicView("dim_date"),
    PublicView("dim_order_status"),
    PublicView("fact_order_history", query=True),
    PublicView("fact_order_item_history", query=True),
    PublicView("current_orders", NOT_DELETED, hidden=("is_open",)),
    PublicView("open_orders", "is_open AND NOT _is_deleted", table="current_orders"),
    PublicView("current_order_items", NOT_DELETED),
    PublicView("open_orders_by_delivery_date_and_status", query=True),
    PublicView("top_delivery_dates_by_open_orders", query=True),
    PublicView("pending_items_by_product", query=True),
    PublicView("top_customers_by_pending_orders", query=True),
    PublicView(FRESHNESS, query=True),
)


class UnknownColumnError(ValueError):
    def __init__(self, table: str, columns: Iterable[str]) -> None:
        super().__init__(f"{table} has no columns named {', '.join(columns)}")


def public_columns(table: GoldTable, hidden: Iterable[str] = ()) -> list[ColumnDefinition]:
    """The columns of the public view of `table`: all but the pipeline-only ones and the
    `hidden` ones it keeps internal."""
    internal = set(hidden)
    unknown = sorted(internal - {column.name for column in table.columns})
    if unknown:
        raise UnknownColumnError(table.name, unknown)
    left_out = PIPELINE_ONLY_COLUMNS | internal
    return [column for column in table.columns if column.name not in left_out]


def view_query(
    view: PublicView,
    tables: Mapping[str, GoldTable],
    internal: str,
    templates: Traversable = TEMPLATES,
) -> str:
    """The view's query over the gold_internal tables in the namespace `internal`:
    `gold_internal` in PostgreSQL, `<catalog>.gold_internal` in the lake."""
    template = view.name if view.query else PROJECTION
    text = (templates / f"{template}.sql.j2").read_text(encoding="utf-8")
    columns = [column.name for column in public_columns(tables[view.name], view.hidden)]
    rendered = SQL.from_string(text).render(
        internal=internal, source=view.source, columns=columns, condition=view.condition
    )
    return rendered.strip("\n")
