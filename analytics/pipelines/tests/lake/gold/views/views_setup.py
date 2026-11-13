from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.lake import IcebergGold, described_schema
from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views
from pipelines.settings import LakeSettings

FIRST_CHANGE = datetime(2026, 11, 1, 10, 0)
DELIVERY_DAY = date(2026, 11, 5)

type GoldRow = dict[str, object]


def at(sequence: int) -> datetime:
    """Commit time of the change at `sequence`: one minute per position, in commit order."""
    return FIRST_CHANGE + timedelta(minutes=sequence)


def customer_version(
    key: int, sequence: int, *, folded: bool = False, deleted: bool = False, current: bool = False
) -> GoldRow:
    return {
        "customer_key": key * 1000 + sequence,
        "customer_id": key,
        "customer_name": f"Customer {key}",
        "_commit_sequence": sequence,
        "_valid_from": at(sequence),
        "_is_deleted": deleted,
        "_is_folded": folded,
        "_is_current": current,
    }


def current_order(
    key: int, sequence: int, *, is_open: bool = True, deleted: bool = False, status_key: int = 1
) -> GoldRow:
    return {
        "order_id": key,
        "status_key": status_key,
        "is_open": is_open,
        "customer_id": 1,
        "delivery_date": DELIVERY_DAY,
        "_commit_sequence": sequence,
        "_changed_at": at(sequence),
        "_is_deleted": deleted,
    }


def order_change(key: int, sequence: int, *, folded: bool = False) -> GoldRow:
    return {
        "order_id": key,
        "_commit_sequence": sequence,
        "_changed_at": at(sequence),
        "_change_op": "update",
        "status_key": 1,
        "_is_deleted": False,
        "_is_folded": folded,
    }


@dataclass(frozen=True, slots=True)
class GoldLake:
    """The gold tables in an in-memory Iceberg catalog, which stores views, with the gold
    views over them."""

    spark: SparkSession
    lake: LakeSettings

    @classmethod
    def create(cls, spark: SparkSession, folder: Path, catalog: str) -> GoldLake:
        lake = LakeSettings(root=f"file://{folder}/lake", checkpoint_root="unused", catalog=catalog)
        gold = cls(spark=spark, lake=lake)
        gold.tables.ensure()
        return gold

    @property
    def tables(self) -> IcebergGold:
        return IcebergGold(
            self.spark, STORED, {name: self.lake.gold_table(name) for name in STORED}
        )

    @property
    def views(self) -> GoldLakeViews:
        return GoldLakeViews(self.lake)

    def refresh(self) -> list[str]:
        return refresh_gold_views(self.spark, self.views)

    def append(self, table: str, *rows: GoldRow) -> None:
        """Append rows to a gold table; the columns a row leaves out are null."""
        schema = described_schema(STORED[table].table)
        full = [tuple(row.get(name) for name in schema.fieldNames()) for row in rows]
        self.spark.createDataFrame(full, schema).writeTo(self.lake.gold_table(table)).append()

    def view(self, name: str) -> str:
        return self.views.view(name)

    def rows(self, name: str, columns: str) -> list[tuple[object, ...]]:
        """The `columns` (comma-separated) of every row of the view, ordered by them."""
        selected = [column.strip() for column in columns.split(",")]
        ordered = self.spark.table(self.view(name)).select(*selected).orderBy(*selected)
        return [tuple(row) for row in ordered.collect()]
