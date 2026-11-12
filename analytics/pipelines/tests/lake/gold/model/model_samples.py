from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import BooleanType, LongType, StructField, StructType, TimestampType

from pipelines.lake.gold.dimensions import load_dimensions, load_facts
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.model.columns import VERSION_COLUMNS
from pipelines.lake.gold.model.seeds import calendar_days, order_statuses
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VersionColumn
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.shared.types import SPARK_TYPES
from pipelines.schemas.definitions import ColumnDefinition
from pipelines.schemas.gold_definitions import GoldTable

CUSTOMERS = load_dimensions()["dim_customer"]
PRODUCTS = load_dimensions()["dim_product"]
ORDERS = load_facts()["fact_order_history"]
ITEMS = load_facts()["fact_order_item_history"]
(CUSTOMER,) = ORDERS.as_of
(PRODUCT,) = ITEMS.as_of

START = datetime(2026, 11, 8, 12, 0)  # Spark returns naive timestamps in UTC
ORDER_DATE = date(2026, 11, 1)
DELIVERY_DATE = date(2026, 11, 20)
VERSION_FIELDS = (
    StructField(VersionColumn.IS_FOLDED, BooleanType()),
    StructField(VersionColumn.VERSION_NUMBER, LongType()),
    StructField(VersionColumn.VALID_TO, TimestampType()),
    StructField(VersionColumn.IS_CURRENT, BooleanType()),
)

type Values = Mapping[str, object]


def at(minute: int) -> datetime:
    return START + timedelta(minutes=minute)


def schema(columns: Iterable[ColumnDefinition], extra: Iterable[StructField] = ()) -> StructType:
    return StructType(
        [*(StructField(column.name, SPARK_TYPES[column.type]) for column in columns), *extra]
    )


def frame(spark: SparkSession, structure: StructType, rows: Iterable[Values]) -> DataFrame:
    return spark.createDataFrame(
        [tuple(row.get(field.name) for field in structure) for row in rows], structure
    )


def staged(
    spark: SparkSession, load: Scd2Dimension | ChangeFact, rows: Iterable[Values]
) -> DataFrame:
    """A batch of staged rows, as `staged_rows` selects them from silver."""
    return frame(spark, schema(load.staged_columns), rows)


def stored(
    spark: SparkSession, table: GoldTable, rows: Iterable[Values] = (), *, versioned: bool = True
) -> DataFrame:
    """Stored rows of a gold table, with the version columns when `versioned`."""
    columns = [column for column in table.columns if column.name not in VERSION_COLUMNS]
    return frame(spark, schema(columns, VERSION_FIELDS if versioned else ()), rows)


def customer(
    customer_id: int, sequence: int, name: str, *, deleted: bool = False, minute: int | None = None
) -> Values:
    return {
        "customer_id": customer_id,
        "customer_name": name,
        "customer_address": "Main Street",
        "is_active": True,
        "_commit_sequence": sequence,
        "_content_hash": f"hash {name}",
        "_is_deleted": deleted,
        "_valid_from": at(sequence if minute is None else minute),
    }


def product(
    product_id: int, sequence: int, price: str, *, deleted: bool = False, minute: int | None = None
) -> Values:
    return {
        "product_id": product_id,
        "product_name": "Widget",
        "barcode": "0001",
        "unit_price": Decimal(price),
        "is_active": True,
        "_commit_sequence": sequence,
        "_content_hash": f"hash {price}",
        "_is_deleted": deleted,
        "_valid_from": at(sequence if minute is None else minute),
    }


def order(
    order_id: int,
    sequence: int,
    status: str | None = "PENDING",
    *,
    customer_id: int | None = 1,
    delivery_date: date | None = DELIVERY_DATE,
    deleted: bool = False,
    minute: int | None = None,
) -> Values:
    return {
        "order_id": order_id,
        "_commit_sequence": sequence,
        "_changed_at": at(sequence if minute is None else minute),
        "_change_op": "delete" if deleted else "update",
        "customer_id": customer_id,
        "_content_hash": f"hash {status} {customer_id} {delivery_date}",
        "_is_deleted": deleted,
        "order_date": ORDER_DATE,
        "delivery_date": delivery_date,
        "status": status,
    }


def item(
    order_item_id: int,
    sequence: int,
    *,
    order_id: int | None = 1,
    product_id: int | None = 1,
    quantity: int | None = 2,
    deleted: bool = False,
    minute: int | None = None,
) -> Values:
    return {
        "order_item_id": order_item_id,
        "_commit_sequence": sequence,
        "_changed_at": at(sequence if minute is None else minute),
        "_change_op": "delete" if deleted else "update",
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "_content_hash": f"hash {order_id} {product_id} {quantity}",
        "_is_deleted": deleted,
    }


def seeded(spark: SparkSession) -> Mapping[str, DataFrame]:
    return {
        "dim_order_status": order_statuses(spark),
        "dim_date": calendar_days(spark, date(2026, 1, 1), date(2026, 12, 31)),
    }


def rows_of(frame: DataFrame, *columns: str) -> list[tuple[object, ...]]:
    """The values of `columns` in every row, sorted, with None first."""
    values = [tuple(row[column] for column in columns) for row in frame.collect()]
    return sorted(values, key=lambda value: tuple((v is not None, v) for v in value))


def merged(stored_rows: DataFrame, changes: DataFrame, keys: Sequence[str]) -> DataFrame:
    """`stored_rows` after merging `changes` on `keys`, as the writer would."""
    kept = stored_rows.join(changes.select(*keys), list(keys), "left_anti")
    return kept.select(*stored_rows.columns).unionByName(changes.select(*stored_rows.columns))


def only(frame: DataFrame) -> Row:
    (row,) = frame.collect()
    return row


def by_sequence(frame: DataFrame) -> dict[int, Row]:
    return {row[COMMIT_SEQUENCE]: row for row in frame.collect()}
