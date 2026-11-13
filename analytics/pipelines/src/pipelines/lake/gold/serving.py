from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import LiteralString, cast

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.facts import FactColumn
from pipelines.lake.gold.postgres import (
    COPY_LOCK,
    ConnectionFactory,
    run_in_transaction,
    transaction,
)
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, postgres_string
from pipelines.lake.gold.stored import StoredTable

SYNC_STATE = f"{INTERNAL_SCHEMA}.sync_state"
SYNC_STATE_QUERY = f"SELECT table_name, snapshot_id FROM {SYNC_STATE}"

type StageWriter = Callable[[DataFrame, str], None]


class Copy(StrEnum):
    """How rows are copied from a staging table into their gold_internal table."""

    UPSERT = "upsert"
    """Insert, or update every column of the row with the same key."""
    RELOAD = "reload"
    """Truncate the table, then insert the staged ones."""


@dataclass(frozen=True, slots=True)
class Change:
    """Rows to copy into a gold_internal table, and how."""

    table: StoredTable
    rows: DataFrame
    copy: Copy = Copy.UPSERT


def _columns(names: Sequence[str]) -> str:
    return ", ".join(names)


def insert_statement(table: StoredTable) -> str:
    columns = _columns(table.columns)
    return (
        f"INSERT INTO {table.relation} ({columns}) SELECT {columns} FROM {table.staging_relation}"
    )


def upsert_statement(table: StoredTable) -> str:
    """Insert the staged rows, updating every other column of a row whose key is stored."""
    key = table.postgres_key
    updated = [column for column in table.columns if column not in key]
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in updated)
    action = f"DO UPDATE SET {assignments}" if updated else "DO NOTHING"
    return f"{insert_statement(table)} ON CONFLICT ({_columns(key)}) {action}"


def copy_statements(change: Change) -> list[str]:
    """The statements that copy a staged change into its table."""
    table = change.table
    if change.copy is Copy.UPSERT:
        return [upsert_statement(table)]
    return [f"TRUNCATE {table.relation}", insert_statement(table)]


def sync_statement(snapshots: Mapping[str, int | None]) -> str:
    """Record the Iceberg snapshot of each table whose rows the transaction copied."""
    values = ", ".join(
        f"({postgres_string(name)}, {'NULL' if snapshot is None else int(snapshot)}, now())"
        for name, snapshot in snapshots.items()
    )
    return (
        f"INSERT INTO {SYNC_STATE} (table_name, snapshot_id, synced_at) VALUES {values}"
        " ON CONFLICT (table_name) DO UPDATE"
        " SET snapshot_id = EXCLUDED.snapshot_id, synced_at = EXCLUDED.synced_at"
    )


def staged(change: Change, retained_from: datetime) -> DataFrame:
    """The rows of a change as its staging table holds them. History older than the retention
    window is left out, so a late change re-versioning an old one never brings a dropped month
    back."""
    rows = change.rows
    if change.table.history:
        rows = rows.where(F.col(FactColumn.CHANGED_AT) >= F.lit(retained_from))
    return rows.select(*(column.name for column in change.table.staging_table.columns))


class PostgresServing:
    """The serving copy of gold in PostgreSQL: every change is staged by Spark's JDBC writer,
    then copied into gold_internal in one transaction, with the Iceberg snapshots it holds.
    Each load copies only its own tables, so the loads copy at the same time."""

    def __init__(
        self,
        stage: StageWriter,
        connect: ConnectionFactory,
        retained_from: Callable[[], datetime],
    ) -> None:
        self.stage = stage
        self.connect = connect
        self.retained_from = retained_from

    def synced(self) -> dict[str, int | None]:
        """The Iceberg snapshot gold_internal holds, by table."""
        with transaction(self.connect) as connection:
            rows = connection.execute(cast("LiteralString", SYNC_STATE_QUERY)).fetchall()
        return {
            str(name): None if snapshot is None else int(str(snapshot)) for name, snapshot in rows
        }

    def apply(self, changes: Sequence[Change], snapshots: Mapping[str, int | None]) -> None:
        """Copy the changes and record the snapshots they hold, in one transaction."""
        if not snapshots:
            return
        retained_from = self.retained_from()
        for change in changes:
            self.stage(staged(change, retained_from), change.table.staging_relation)
        statements = [COPY_LOCK]
        for change in changes:
            statements += copy_statements(change)
        statements.append(sync_statement(snapshots))
        run_in_transaction(self.connect, statements)
