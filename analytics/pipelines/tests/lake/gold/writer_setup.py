from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import TracebackType

from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.gold.batch import Reader, Write
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.serving import Change
from pipelines.lake.gold.stored import StoredTable

SEEDS = STORED["dim_order_status"]
BY_PRODUCT = STORED["pending_items_by_product"]
TOP = STORED["top_delivery_dates_by_open_orders"]
TABLES = {table.name: table for table in (SEEDS, BY_PRODUCT, TOP)}
SEED_SCHEMA = "status_key int, status_code string, is_open boolean"
REPORT_SCHEMA = "product_id long, pending_items long, pending_quantity long"
CONFLICT = "org.apache.iceberg.exceptions.CommitFailedException: Cannot commit changes"


class ConflictError(RuntimeError):
    pass


def seeds(spark: SparkSession, *rows: tuple[int, str, bool]) -> DataFrame:
    return spark.createDataFrame(list(rows), SEED_SCHEMA)


def report(spark: SparkSession, *rows: tuple[int | None, int, int]) -> DataFrame:
    return spark.createDataFrame(list(rows), REPORT_SCHEMA)


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

    def unchanged(self, table: StoredTable, rows: DataFrame) -> bool:
        return rows.isEmpty()

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

    def writes(self, load: str, batch: DataFrame, read: Reader) -> list[Write]:
        self.log.append(f"compute {load}")
        return [Write(self.tables[name], rows) for name, rows in self.planned]


@dataclass
class RecordingLock:
    log: list[str]

    def __enter__(self) -> object:
        self.log.append("lock")
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.log.append("unlock")
