from collections.abc import Callable, Iterable, Mapping, Sequence
from importlib.resources import files
from typing import NamedTuple

from jinja2 import Environment, StrictUndefined
from pyspark.sql import SparkSession

from pipelines.lake.gold.ddl import PIPELINE_ONLY_COLUMNS
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.lake.gold.facts import AsOfLookup, ChangeFact, FactColumn, KeyLookup
from pipelines.lake.gold.scd2 import Scd2Column, Scd2Dimension
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.tables import TABLE_TEMPLATES, sql_string
from pipelines.lake.shared.views import ViewDefinition, ensure_view
from pipelines.lake.silver.views import HISTORY
from pipelines.schemas.definitions import ColumnDefinition
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables
from pipelines.settings import GoldSettings, LakeSettings, SilverSettings

SQL = Environment(undefined=StrictUndefined, autoescape=False, trim_blocks=True, lstrip_blocks=True)
SEEDED = ("dim_date", "dim_order_status")
FRESHNESS = "data_freshness"
OPEN_ORDERS = "open_orders"
BUSINESS_VIEWS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)
INTERNAL_COLUMNS: Mapping[str, frozenset[str]] = {"current_orders": frozenset({"is_open"})}
HISTORY_COLUMNS = (MetadataColumn.VALID_TO, MetadataColumn.IS_CURRENT)
DIMENSION_RANGE_COLUMNS = (Scd2Column.VALID_FROM, *HISTORY_COLUMNS)
COMMIT_SEQUENCE = str(Scd2Column.COMMIT_SEQUENCE)


def _render(template: str, **values: object) -> str:
    text = (files("pipelines.lake.gold.views") / "sql" / f"{template}.sql.j2").read_text(
        encoding="utf-8"
    )
    return SQL.from_string(text).render(**values).rstrip("\n")


def _selected(source: str, column: str) -> str:
    return source if source == column else f"{source} AS {column}"


class KeyJoin(NamedTuple):
    alias: str
    relation: str
    code: str
    source: str


class AsOfJoin(NamedTuple):
    name: str
    versions: str
    dimension_key: str
    business_key: str
    columns: tuple[tuple[str, str], ...]


class GoldLakeViews:
    def __init__(
        self,
        lake: LakeSettings,
        tables: Mapping[str, GoldTable] | None = None,
        dimensions: Iterable[Scd2Dimension] = DIMENSIONS,
        facts: Iterable[ChangeFact] = FACTS,
        *,
        relation: Callable[[str, str], str] | None = None,
        version_keys: bool = False,
    ) -> None:
        self.lake = lake
        self.tables = tables if tables is not None else load_gold_tables()
        self.dimensions = tuple(dimensions)
        self.facts = tuple(facts)
        self.relation = relation or lake.table
        self.surrogate_keys = frozenset(d.surrogate_key for d in self.dimensions)
        self.hidden = PIPELINE_ONLY_COLUMNS | (frozenset() if version_keys else self.surrogate_keys)

    @property
    def namespace(self) -> str:
        return f"{self.lake.catalog}.{GoldSettings.LAYER}"

    def view(self, name: str) -> str:
        return self.relation(GoldSettings.LAYER, name)

    def _silver(self, name: str) -> str:
        return self.relation(SilverSettings.LAYER, name)

    def _silver_history(self, load: Scd2Dimension | ChangeFact) -> str:
        return self._silver(TABLE_TEMPLATES[HISTORY].table_name(load.entity))

    def public_columns(self, table: GoldTable) -> tuple[ColumnDefinition, ...]:
        """The columns of the public PostgreSQL view of `table`, without the surrogate keys."""
        hidden = self.hidden | INTERNAL_COLUMNS.get(table.name, frozenset())
        kept = (column for column in table.columns if column.name not in hidden)
        return tuple(kept)

    def _definition(
        self, table: GoldTable, columns: Sequence[ColumnDefinition], query: str
    ) -> ViewDefinition:
        return ViewDefinition(
            comment=table.view_description,
            columns={column.name: column for column in columns},
            query=query,
        )

    def seeded(self, name: str) -> ViewDefinition:
        table = self.tables[name]
        return self._definition(table, table.columns, _render(name))

    def dimension(self, dimension: Scd2Dimension) -> ViewDefinition:
        columns = self.public_columns(dimension.table)
        sources = dict(dimension.silver_columns) | {dimension.surrogate_key: COMMIT_SEQUENCE}
        selected = [
            column.name
            if column.name in DIMENSION_RANGE_COLUMNS
            else _selected(sources[column.name], column.name)
            for column in columns
        ]
        query = _render("dimension", columns=selected, history=self._silver_history(dimension))
        return self._definition(dimension.table, columns, query)

    def _key_joins(self, fact: ChangeFact, names: set[str]) -> tuple[list[KeyJoin], dict[str, str]]:
        joins: list[KeyJoin] = []
        expressions: dict[str, str] = {}
        for lookup in (lookup for lookup in fact.lookups if lookup.column in names):
            join = KeyJoin(
                alias=f"{lookup.column}_lookup",
                relation=self.view(lookup.dimension.name),
                code=lookup.code,
                source=fact.silver_columns[lookup.source],
            )
            joins.append(join)
            expressions[lookup.column] = self._key_expression(join, lookup)
        return joins, expressions

    @staticmethod
    def _key_expression(join: KeyJoin, lookup: KeyLookup) -> str:
        matched = f"{join.alias}.{lookup.key}"
        if lookup.fallback is None:
            return f"{matched} AS {lookup.column}"
        fallback = (
            f"(SELECT {lookup.key} FROM {join.relation}"
            f" WHERE {lookup.code} = {sql_string(lookup.fallback)})"
        )
        return f"coalesce({matched}, {fallback}) AS {lookup.column}"

    def _as_of_joins(
        self, fact: ChangeFact, names: set[str]
    ) -> tuple[list[AsOfJoin], dict[str, str]]:
        joins: list[AsOfJoin] = []
        expressions: dict[str, str] = {}
        for lookup in (lookup for lookup in fact.as_of if set(lookup.fact_columns) & names):
            join = self._as_of_join(fact, lookup)
            joins.append(join)
            expressions |= {column: f"{join.name}.{column}" for column, _ in join.columns}
            for multiplied in lookup.multiplied:
                expressions[multiplied.column] = (
                    f"CAST(CAST({join.name}.{multiplied.looked_up} AS DECIMAL(27, 10))"
                    f" * change.{fact.silver_columns[multiplied.factor]} AS DECIMAL(38, 10))"
                    f" AS {multiplied.column}"
                )
        return joins, expressions

    def _as_of_join(self, fact: ChangeFact, lookup: AsOfLookup) -> AsOfJoin:
        dimension = lookup.dimension
        sources = dict(dimension.silver_columns) | {dimension.surrogate_key: COMMIT_SEQUENCE}
        columns = tuple(
            (fact_column, sources[column])
            for fact_column, column in lookup.columns
            if fact_column not in self.hidden
        )
        return AsOfJoin(
            name=f"{dimension.name}_at_change",
            versions=self._silver_history(dimension),
            dimension_key=sources[dimension.key],
            business_key=fact.silver_columns[lookup.business_key],
            columns=columns,
        )

    def _fact(
        self,
        fact: ChangeFact,
        table: GoldTable,
        columns: Sequence[ColumnDefinition],
        *,
        latest: bool,
    ) -> ViewDefinition:
        names = {column.name for column in columns}
        key_joins, expressions = self._key_joins(fact, names)
        as_of_joins, looked_up = self._as_of_joins(fact, names)
        expressions |= looked_up
        expressions[MetadataColumn.VALID_TO] = f"change.{MetadataColumn.VALID_TO}"
        expressions[MetadataColumn.IS_CURRENT] = (
            f"change.{MetadataColumn.IS_CURRENT} AND NOT change.{FactColumn.IS_DELETED}"
            f" AS {MetadataColumn.IS_CURRENT}"
        )
        sources = fact.silver_columns
        selected = [
            expressions.get(column.name) or f"change.{_selected(sources[column.name], column.name)}"
            for column in columns
        ]
        query = _render(
            "fact",
            latest=latest,
            source=self._silver(fact.entity.name) if latest else self._silver_history(fact),
            key=sources[fact.key],
            columns=selected,
            key_joins=key_joins,
            as_of_joins=as_of_joins,
        )
        return self._definition(table, columns, query)

    def history(self, fact: ChangeFact) -> ViewDefinition:
        return self._fact(fact, fact.history, self.public_columns(fact.history), latest=False)

    def current(self, fact: ChangeFact) -> ViewDefinition:
        return self._fact(fact, fact.current, self.public_columns(fact.current), latest=True)

    def derived(self, name: str) -> ViewDefinition:
        table = self.tables[name]
        columns = [column.name for column in table.columns]
        views = {relation: self.view(relation) for relation in self.tables}
        return self._definition(table, table.columns, _render(name, views=views, columns=columns))

    def freshness(self) -> ViewDefinition:
        table = self.tables[FRESHNESS]
        loads = [*self.dimensions, *self.facts]
        sources = [(load.entity.name, self._silver(load.entity.name)) for load in loads]
        return self._definition(table, table.columns, _render(FRESHNESS, sources=sources))

    def definitions(self) -> Mapping[str, ViewDefinition]:
        """Every view by name, each after the views it reads."""
        views = {self.view(name): self.seeded(name) for name in SEEDED}
        views |= {self.view(d.name): self.dimension(d) for d in self.dimensions}
        views |= {self.view(fact.history.name): self.history(fact) for fact in self.facts}
        views |= {self.view(fact.current.name): self.current(fact) for fact in self.facts}
        views |= {self.view(name): self.derived(name) for name in (OPEN_ORDERS, *BUSINESS_VIEWS)}
        views[self.view(FRESHNESS)] = self.freshness()
        return views


def refresh_gold_views(spark: SparkSession, views: GoldLakeViews) -> list[str]:
    """Create or replace the gold views whose query or descriptions changed; returns the
    statements that ran."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {views.namespace}")
    statements = []
    for view, definition in views.definitions().items():
        statement = ensure_view(spark, view, definition)
        if statement is not None:
            statements.append(statement)
    return statements
