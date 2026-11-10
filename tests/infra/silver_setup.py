from dataclasses import dataclass

from pipelines.lake.columns import MetadataColumn
from tests.infra.bronze_setup import (
    ENTITIES,
    ENV,
    Rows,
    Tables,
    change,
    land,
    read_tables,
    run_bronze_and_read_tables,
    submit,
)
from tests.infra.stack import Stack

SILVER_APP = "analytics/pipelines/src/pipelines/silver/job.py"
SILVER_TABLES = [f"lake.silver.{entity}" for entity in ENTITIES]
NEXT_OFFSET = 4


@dataclass(frozen=True, slots=True)
class SilverRuns:
    bronze: Tables
    first: Tables
    restarted: Tables
    idle: Tables


def commit_sequence(row: dict[str, str | None]) -> int:
    return int(str(row[MetadataColumn.COMMIT_SEQUENCE]))


def commit_sequences(rows: Rows) -> list[int]:
    return [commit_sequence(row) for row in rows]


def run_silver_and_read_tables(stack: Stack) -> Tables:
    submit(stack, SILVER_APP, "--available-now")
    return read_tables(stack, SILVER_TABLES, commit_sequence)


def run_silver_scenario(stack: Stack, bronze: Tables) -> SilverRuns:
    """Version the changes in `bronze`, then an update bronze appends after them, then run once
    more with nothing new."""
    first = run_silver_and_read_tables(stack)

    land(stack, "batch-3", {entity: [change(entity, NEXT_OFFSET, "u")] for entity in ENTITIES})
    run_bronze_and_read_tables(stack)
    restarted = run_silver_and_read_tables(stack)

    idle = run_silver_and_read_tables(stack)
    return SilverRuns(bronze=bronze, first=first, restarted=restarted, idle=idle)


def run_silver_without_bronze_tables(stack: Stack) -> tuple[int, str]:
    # Once the bronze scenario has run, every bronze table exists in the lake catalog, so this
    # run reads from Spark's built-in catalog, which has none.
    return stack.run_app(
        SILVER_APP, "--available-now", env=ENV | {"ICEBERG_CATALOG": "spark_catalog"}
    )
