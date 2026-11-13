from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.gold.batch import Reader, Write
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.serving import Change
from pipelines.lake.gold.stored import StoredTable

SEEDS = STORED["dim_order_status"]
ITEMS = STORED["current_order_items"]
OTHER = STORED["dim_customer"]
TABLES = {table.name: table for table in (SEEDS, ITEMS, OTHER)}
SEED_SCHEMA = "status_key int, status_code string, is_open boolean"
ITEM_SCHEMA = (
    "order_item_id long, order_id long, product_id long, quantity int, _commit_sequence long,"
    " _changed_at timestamp, _is_deleted boolean"
)
CHANGED_AT = datetime(2026, 11, 1, 10, 0)
CONFLICT = "org.apache.iceberg.exceptions.CommitFailedException: Cannot commit changes"


class ConflictError(RuntimeError):
    pass


def seeds(spark: SparkSession, *rows: tuple[int, str, bool]) -> DataFrame:
    return spark.createDataFrame(list(rows), SEED_SCHEMA)


def items(spark: SparkSession, *rows: tuple[int, int]) -> DataFrame:
    """Current order item rows, each an (order_item_id, quantity) pair."""
    return spark.createDataFrame(
        [(key, 1, 1, quantity, key, CHANGED_AT, False) for key, quantity in rows], ITEM_SCHEMA
    )


@dataclass
class FakeLake:
    """Iceberg tables as DataFrames, with a new snapshot id for every write."""

    tables: dict[str, DataFrame]
    log: list[str]
    conflicts: dict[str, int] = field(default_factory=dict)
    snapshots: dict[str, int] = field(default_factory=dict)
    history: dict[tuple[str, int], DataFrame] = field(default_factory=dict)

    def read(self, name: str) -> DataFrame:
        return self.tables[name]

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None:
        return self.history.get((name, snapshot_id))

    def snapshot(self, name: str) -> int | None:
        return self.snapshots.get(name)

    def write(self, table: StoredTable, rows: DataFrame) -> None:
        self.log.append(f"lake write {table.name} {rows.count()}")
        if self.conflicts.get(table.name, 0) > 0:
            self.conflicts[table.name] -= 1
            raise ConflictError(CONFLICT)
        snapshot = self.snapshots.get(table.name, 0) + 1
        self.snapshots[table.name] = snapshot
        self.tables[table.name] = rows
        self.history[(table.name, snapshot)] = rows


@dataclass
class FakeServing:
    log: list[str]
    synced_snapshots: dict[str, int | None] = field(default_factory=dict)
    changes: list[list[Change]] = field(default_factory=list)

    def synced(self) -> Mapping[str, int | None]:
        return dict(self.synced_snapshots)

    def apply(self, changes: Sequence[Change], snapshots: Mapping[str, int | None]) -> None:
        described = ", ".join(f"{c.table.name} {c.copy} {c.rows.count()}" for c in changes)
        self.log.append(f"copy [{described}] {dict(snapshots)}")
        self.changes.append(list(changes))
        self.synced_snapshots |= snapshots


@dataclass
class FakeBatches:
    """Hands out the same writes for every batch, logging each computation."""

    planned: list[tuple[str, DataFrame]]
    log: list[str]
    tables: Mapping[str, StoredTable] = field(default_factory=lambda: TABLES)

    def tables_of(self, load: str) -> tuple[str, ...]:
        return (SEEDS.name, ITEMS.name)

    def staged(self, load: str, batch: DataFrame) -> DataFrame:
        self.log.append(f"stage {load}")
        return batch

    def writes(self, load: str, staged: DataFrame, read: Reader) -> list[Write]:
        self.log.append(f"compute {load}")
        return [Write(self.tables[name], rows) for name, rows in self.planned]
