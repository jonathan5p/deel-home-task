from collections.abc import Iterable, Mapping, Sequence

from pyspark.sql import SparkSession

from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.public import PUBLIC_VIEWS, PublicView, public_columns, view_query
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA
from pipelines.lake.shared.views import ViewDefinition, ensure_view
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables
from pipelines.settings import GoldSettings, LakeSettings


class MissingGoldTablesError(LookupError):
    def __init__(self, tables: Iterable[str]) -> None:
        super().__init__(
            f"The gold views read tables that do not exist yet: {', '.join(tables)}."
            " Start the gold job first."
        )


class GoldLakeViews:
    """The gold views in the lake: the public views of PostgreSQL's gold schema, rendered from
    the same templates, with the same names and columns, over the Iceberg tables the gold job
    writes."""

    def __init__(
        self,
        lake: LakeSettings,
        tables: Mapping[str, GoldTable] | None = None,
        public: Sequence[PublicView] = PUBLIC_VIEWS,
        stored: Iterable[str] = STORED,
    ) -> None:
        self.lake = lake
        self.tables = tables if tables is not None else load_gold_tables()
        self.public = tuple(public)
        self.stored = tuple(stored)

    @property
    def namespace(self) -> str:
        return f"{self.lake.catalog}.{GoldSettings.LAYER}"

    @property
    def internal(self) -> str:
        """The namespace of the Iceberg tables the views read."""
        return f"{self.lake.catalog}.{INTERNAL_SCHEMA}"

    def view(self, name: str) -> str:
        return self.lake.table(GoldSettings.LAYER, name)

    def sources(self) -> list[str]:
        """The Iceberg tables the views read: every table the gold job writes."""
        return [self.lake.gold_table(name) for name in self.stored]

    def definition(self, view: PublicView) -> ViewDefinition:
        table = self.tables[view.name]
        columns = public_columns(table, view.hidden)
        return ViewDefinition(
            comment=table.view_description,
            columns={column.name: column for column in columns},
            query=view_query(view, self.tables, self.internal),
        )

    def definitions(self) -> Mapping[str, ViewDefinition]:
        """Every view by name."""
        return {self.view(view.name): self.definition(view) for view in self.public}


def refresh_gold_views(spark: SparkSession, views: GoldLakeViews) -> list[str]:
    """Create or replace the gold views whose query or descriptions changed; returns the
    statements that ran. Spark resolves a view's query when it is created, so the gold tables
    must exist first."""
    missing = [table for table in views.sources() if not spark.catalog.tableExists(table)]
    if missing:
        raise MissingGoldTablesError(missing)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {views.namespace}")
    statements = []
    for view, definition in views.definitions().items():
        statement = ensure_view(spark, view, definition)
        if statement is not None:
            statements.append(statement)
    return statements


def drop_gold_views(spark: SparkSession, views: GoldLakeViews) -> None:
    """Drop every gold view that exists."""
    for view in views.definitions():
        spark.sql(f"DROP VIEW IF EXISTS {view}")
