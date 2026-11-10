from dataclasses import dataclass

from pipelines.lake.columns import MetadataColumn
from tests.infra.bronze_setup import (
    ENTITIES,
    ENV,
    FIRST_LSN,
    KEYS,
    LAKE_KEY,
    Json,
    Rows,
    Tables,
    change,
    land,
    read_tables,
    run_bronze_and_read_tables,
    submit,
)
from tests.infra.stack import Stack

VIEWS = ("history", "current")
type SqlResults = dict[str, list[list[str]]]

SILVER_APP = "analytics/pipelines/src/pipelines/silver/job.py"
VIEWS_APP = "analytics/pipelines/src/pipelines/silver/views_job.py"
SILVER_TABLES = [f"lake.silver.{entity}" for entity in ENTITIES]
SILVER_VIEWS = [f"lake.silver.{entity}_{view}" for entity in ENTITIES for view in VIEWS]
VIEWS_SQL = "tests/infra/sql/silver_views.sql"
VIEWS_SQL_ENTITY = "customers"
NEXT_OFFSET = 4
VERSIONED_UPDATE_OFFSET = 1


@dataclass(frozen=True, slots=True)
class SilverRuns:
    bronze: Tables
    first: Tables
    bronze_extended: Tables
    restarted: Tables
    idle: Tables
    first_view_files: dict[str, str]
    idle_view_files: dict[str, str]
    spark_sql: SqlResults


def commit_sequence(row: dict[str, str | None]) -> int:
    return int(str(row[MetadataColumn.COMMIT_SEQUENCE]))


def commit_sequences(rows: Rows) -> list[int]:
    return [commit_sequence(row) for row in rows]


def third_batch(entity: str) -> list[Json]:
    """An update, the same update delivered again, and the update silver versioned in its first
    run delivered again: each redelivery has a new offset but the same commit sequence."""
    return [
        change(entity, NEXT_OFFSET, "u"),
        change(entity, NEXT_OFFSET + 1, "u", lsn=FIRST_LSN + NEXT_OFFSET),
        change(entity, NEXT_OFFSET + 2, "u", lsn=FIRST_LSN + VERSIONED_UPDATE_OFFSET),
    ]


def state_store_files(stack: Stack, entity: str) -> set[str]:
    """The files of the first state partition of the entity's silver query."""
    return stack.lake_entries(f"{LAKE_KEY}/checkpoints/silver/{entity}/state/0/0/")


def run_silver_and_read_tables(stack: Stack) -> Tables:
    """Run the job, then the views job, then read the tables and views."""
    submit(stack, SILVER_APP, "--available-now")
    submit(stack, VIEWS_APP)
    return read_tables(stack, [*SILVER_TABLES, *SILVER_VIEWS], commit_sequence)


def view_metadata_files(stack: Stack) -> dict[str, str]:
    """The current metadata file of every view in the lake catalog, by view name; every change
    to a view writes a new one."""
    listing = stack.sql(
        "iceberg_catalog",
        "select 'lake.' || table_namespace || '.' || table_name, metadata_location"
        " from iceberg_tables where iceberg_type = 'VIEW'",
    )
    return dict(line.split("|", 1) for line in listing.splitlines())


def query_views_with_spark_sql(stack: Stack) -> SqlResults:
    """Run the view queries of one entity with make spark-sql and group the rows by query."""
    key = KEYS[VIEWS_SQL_ENTITY]
    run = stack.run_sql_file(VIEWS_SQL, ENV["LAKE_ROOT"], {"entity": VIEWS_SQL_ENTITY, "key": key})
    assert run.returncode == 0, run.stdout + run.stderr
    grouped: SqlResults = {}
    for line in run.stdout.splitlines():
        label, _, rest = line.partition("\t")
        if rest:
            grouped.setdefault(label, []).append(rest.split("\t"))
    return grouped


def run_silver_scenario(stack: Stack, bronze: Tables) -> SilverRuns:
    """Version the changes in `bronze`, then the third batch bronze appends after them, then run
    once more with nothing new; finally query the views with make spark-sql."""
    first = run_silver_and_read_tables(stack)
    first_view_files = view_metadata_files(stack)

    land(stack, "batch-3", {entity: third_batch(entity) for entity in ENTITIES})
    bronze_extended = run_bronze_and_read_tables(stack)
    restarted = run_silver_and_read_tables(stack)

    idle = run_silver_and_read_tables(stack)
    return SilverRuns(
        bronze=bronze,
        first=first,
        bronze_extended=bronze_extended,
        restarted=restarted,
        idle=idle,
        first_view_files=first_view_files,
        idle_view_files=view_metadata_files(stack),
        spark_sql=query_views_with_spark_sql(stack),
    )


def run_without_upstream_tables(stack: Stack, app: str, *args: str) -> tuple[int, str]:
    # Once the scenarios have run, every bronze and silver table exists in the lake catalog, so
    # this run reads from Spark's built-in catalog, which has none.
    return stack.run_app(app, *args, env=ENV | {"ICEBERG_CATALOG": "spark_catalog"})
