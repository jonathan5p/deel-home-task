from dataclasses import dataclass

from pipelines.lake.columns import MetadataColumn
from tests.infra.bronze_setup import (
    ENTITIES,
    ENV,
    FIRST_LSN,
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

SILVER_APP = "analytics/pipelines/src/pipelines/silver/job.py"
SILVER_TABLES = [f"lake.silver.{entity}" for entity in ENTITIES]
NEXT_OFFSET = 4
VERSIONED_UPDATE_OFFSET = 1


@dataclass(frozen=True, slots=True)
class SilverRuns:
    bronze: Tables
    first: Tables
    bronze_extended: Tables
    restarted: Tables
    idle: Tables


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
    submit(stack, SILVER_APP, "--available-now")
    return read_tables(stack, SILVER_TABLES, commit_sequence)


def run_silver_scenario(stack: Stack, bronze: Tables) -> SilverRuns:
    """Version the changes in `bronze`, then the third batch bronze appends after them, then run
    once more with nothing new."""
    first = run_silver_and_read_tables(stack)

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
    )


def run_silver_without_bronze_tables(stack: Stack) -> tuple[int, str]:
    # Once the bronze scenario has run, every bronze table exists in the lake catalog, so this
    # run reads from Spark's built-in catalog, which has none.
    return stack.run_app(
        SILVER_APP, "--available-now", env=ENV | {"ICEBERG_CATALOG": "spark_catalog"}
    )
