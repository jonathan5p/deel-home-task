from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pyspark.sql import Row, SparkSession

from analytics.pipelines.tests.gold.silver_setup import silver_schema
from pipelines.gold.dimensions import DIMENSIONS, FACTS
from pipelines.gold.lake.views import GoldLakeViews, refresh_gold_views
from pipelines.schemas.definitions import EntityDefinition
from pipelines.settings import LakeSettings, SilverSettings
from pipelines.silver.views import refresh_views

FIRST_CHANGE = datetime(2026, 11, 1, 10, 0)
ENTITIES: dict[str, EntityDefinition] = {
    load.entity.name: load.entity for load in (*DIMENSIONS, *FACTS)
}
ORDER_DAY = date(2026, 11, 1)
DELIVERY_DAY = date(2026, 11, 5)


def at(sequence: int) -> datetime:
    """Commit time of the change at `sequence`: one minute per position, in commit order."""
    return FIRST_CHANGE + timedelta(minutes=sequence)


def change(
    entity: str,
    key: int,
    sequence: int,
    content: dict[str, object],
    *,
    op: str = "update",
    deleted: bool = False,
    changed_at: datetime | None = None,
) -> Row:
    definition = ENTITIES[entity]
    row = dict.fromkeys(silver_schema(definition).fieldNames())
    metadata = {
        "_commit_sequence": sequence,
        "_change_op": op,
        "_changed_at": changed_at or at(sequence),
        "_content_hash": repr(sorted(content.items())),
        "_is_deleted": deleted,
    }
    return Row(**(row | content | {definition.key: key} | metadata))


def customer(
    key: int, sequence: int, name: str, *, op: str = "update", deleted: bool = False
) -> Row:
    content = {"customer_name": name, "customer_address": "Main Street 1", "is_active": True}
    return change("customers", key, sequence, content, op=op, deleted=deleted)


def product(
    key: int,
    sequence: int,
    price: str,
    *,
    op: str = "update",
    deleted: bool = False,
    changed_at: datetime | None = None,
) -> Row:
    content = {"product_name": f"Product {key}", "barcode": str(key), "unity_price": Decimal(price)}
    return change("products", key, sequence, content, op=op, deleted=deleted, changed_at=changed_at)


def order(
    key: int,
    sequence: int,
    status: str | None,
    *,
    op: str = "update",
    deleted: bool = False,
    delivery_date: date | None = DELIVERY_DAY,
    customer_id: int = 1,
    changed_at: datetime | None = None,
) -> Row:
    content = {
        "customer_id": customer_id,
        "order_date": ORDER_DAY,
        "delivery_date": delivery_date,
        "status": status,
    }
    return change("orders", key, sequence, content, op=op, deleted=deleted, changed_at=changed_at)


def item(
    key: int,
    sequence: int,
    product_id: int,
    quantity: int,
    *,
    deleted: bool = False,
    changed_at: datetime | None = None,
    order_id: int = 1,
) -> Row:
    content = {"order_id": order_id, "product_id": product_id, "quantity": quantity}
    return change("order_items", key, sequence, content, deleted=deleted, changed_at=changed_at)


@dataclass(frozen=True, slots=True)
class GoldLake:
    spark: SparkSession
    lake: LakeSettings

    @classmethod
    def create(cls, spark: SparkSession, folder: Path, catalog: str) -> GoldLake:
        lake = LakeSettings(root=f"file://{folder}/lake", checkpoint_root="unused", catalog=catalog)
        gold = cls(spark=spark, lake=lake)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
        for entity in ENTITIES.values():
            table = lake.table(SilverSettings.LAYER, entity.name)
            spark.createDataFrame([], silver_schema(entity)).writeTo(table).create()
        return gold

    @property
    def views(self) -> GoldLakeViews:
        return GoldLakeViews(self.lake)

    @property
    def silver(self) -> SilverSettings:
        return SilverSettings(
            topics=tuple(f"test.operations.{name}" for name in ENTITIES),
            lake=self.lake,
            trigger_seconds=1,
        )

    def refresh(self) -> list[str]:
        """Refresh the silver views, then the gold views; returns the gold statements."""
        refresh_views(self.spark, self.silver)
        return refresh_gold_views(self.spark, self.views)

    def append(self, entity: str, *rows: Row) -> None:
        table = self.lake.table(SilverSettings.LAYER, entity)
        schema = silver_schema(ENTITIES[entity])
        self.spark.createDataFrame(list(rows), schema).writeTo(table).append()

    def view(self, name: str) -> str:
        return self.views.view(name)

    def rows(self, name: str, columns: str, key: str, *keys: int) -> list[tuple[object, ...]]:
        """The `columns` (comma-separated) of the view's rows of the given keys, in the order of
        those columns."""
        selected = [column.strip() for column in columns.split(",")]
        frame = self.spark.table(self.view(name)).where(f"{key} IN ({', '.join(map(str, keys))})")
        ordered = frame.select(*selected).orderBy(*selected)
        return [tuple(row) for row in ordered.collect()]
