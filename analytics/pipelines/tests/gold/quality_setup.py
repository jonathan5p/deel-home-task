from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import BooleanType, DecimalType, StructField, StructType, TimestampType

from analytics.pipelines.tests.gold.silver_setup import CHANGED_AT, silver_schema, silver_version
from pipelines.gold.ddl import PIPELINE_ONLY_COLUMNS
from pipelines.gold.dimensions import load_dimensions, load_facts
from pipelines.gold.quality import (
    COMPARISONS,
    BatchReader,
    CommittedBatch,
    GoldReader,
    GoldReaders,
    SnapshotReader,
)
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.definitions import ColumnType, EntityDefinition
from pipelines.schemas.gold_definitions import load_gold_tables
from pipelines.settings import GoldQualitySettings

ENV = {
    "LAKE_ROOT": "file:///lake",
    "ICEBERG_CATALOG": "catalog",
    "ANALYTICS_DB_URL": "jdbc:postgresql://db/gold",
    "ANALYTICS_DB_USER": "gold",
    "ANALYTICS_DB_PASSWORD": "secret",
}
SETTINGS = GoldQualitySettings.from_env(ENV)
GOLD_TABLES = load_gold_tables()
JDBC_DECIMAL = DecimalType(38, 18)
CUSTOMERS = load_dimensions()["dim_customer"].entity
PRODUCTS = load_dimensions()["dim_product"].entity
ORDERS = load_facts()["fact_order_history"].entity
CUSTOMER_SNAPSHOT = ("catalog.silver.customers", 1)
HISTORY_VIEW_COLUMNS = (
    StructField("_valid_to", TimestampType()),
    StructField("_is_current", BooleanType()),
)


def at(second: int) -> datetime:
    return CHANGED_AT + timedelta(seconds=second)


def batch(snapshot: int, number: int = 0) -> CommittedBatch:
    return CommittedBatch("query", number, ({"snapshot_id": snapshot, "position": 1},))


def silver_frame(spark: SparkSession, entity: EntityDefinition, rows: Sequence[Row]) -> DataFrame:
    return spark.createDataFrame(list(rows), silver_schema(entity))


def view_schema(view: str) -> StructType:
    """The public gold view as JDBC reads it."""
    table = GOLD_TABLES[view]
    fields = [
        StructField(
            column.name,
            JDBC_DECIMAL if column.type is ColumnType.DECIMAL else SPARK_TYPES[column.type],
        )
        for column in table.columns
        if column.name not in PIPELINE_ONLY_COLUMNS
    ]
    added = (
        HISTORY_VIEW_COLUMNS if view.startswith(("fact_", "dim_customer", "dim_product")) else ()
    )
    return StructType([*fields, *added])


def gold_frame(spark: SparkSession, view: str, rows: Sequence[Mapping[str, object]]) -> DataFrame:
    schema = view_schema(view)
    blank = dict.fromkeys(schema.fieldNames())
    return spark.createDataFrame([Row(**(blank | dict(row))) for row in rows], schema)


def checkpoints(batches: Mapping[str, CommittedBatch | None]) -> BatchReader:
    """A reader of the batch each gold stream's checkpoint holds, by stream name."""

    def read(checkpoint: str) -> CommittedBatch | None:
        return batches.get(checkpoint.rsplit("/", 1)[-1])

    return read


def successive(*answers: CommittedBatch | None) -> BatchReader:
    """A reader that answers each call with the next batch, as a running job moves on."""
    remaining: Iterator[CommittedBatch | None] = iter(answers)

    def read(checkpoint: str) -> CommittedBatch | None:
        return next(remaining)

    return read


def snapshots(frames: Mapping[tuple[str, int], DataFrame]) -> SnapshotReader:
    """A reader of silver snapshots by table and snapshot id."""

    def read(spark: SparkSession, table: str, snapshot_id: int) -> DataFrame:
        return frames[(table, snapshot_id)]

    return read


def gold_views(frames: Mapping[str, DataFrame]) -> GoldReader:
    def read(spark: SparkSession, relation: str) -> DataFrame:
        return frames[relation]

    return read


def unread_gold(spark: SparkSession, relation: str) -> DataFrame:
    raise AssertionError(f"{relation} must not be read")


def readers(
    spark: SparkSession,
    silver: Mapping[tuple[str, int], Sequence[Row]],
    gold: Mapping[str, Sequence[Mapping[str, object]]],
    committed: CommittedBatch | None = None,
    planned: CommittedBatch | None = None,
) -> GoldReaders:
    """Readers of the given silver snapshots and gold views, with every gold stream's checkpoint
    at the `committed` and `planned` batches."""
    entities = {"customers": CUSTOMERS, "products": PRODUCTS, "orders": ORDERS}
    frames = {
        (table, snapshot): silver_frame(spark, entities[table.rsplit(".", 1)[-1]], rows)
        for (table, snapshot), rows in silver.items()
    }
    views = {f"gold.{view}": gold_frame(spark, view, rows) for view, rows in gold.items()}
    streams = {comparison.stream: committed for comparison in COMPARISONS}
    plans = {comparison.stream: planned for comparison in COMPARISONS}
    return GoldReaders(
        committed=checkpoints(streams),
        planned=checkpoints(plans),
        read_gold=gold_views(views),
        read_snapshot=snapshots(frames),
    )


def customer_versions() -> list[Row]:
    """Customer 7 changes its name after an unchanged re-snapshot; customer 8 is deleted."""
    return [
        silver_version(
            CUSTOMERS, 7, 10, customer_name="Ada", _changed_at=at(10), _content_hash="a"
        ),
        silver_version(
            CUSTOMERS,
            7,
            20,
            customer_name="Ada",
            _change_op="snapshot",
            _changed_at=at(20),
            _content_hash="a",
        ),
        silver_version(
            CUSTOMERS, 7, 30, customer_name="Ada L.", _changed_at=at(30), _content_hash="b"
        ),
        silver_version(CUSTOMERS, 8, 11, customer_name="Bob", _changed_at=at(11)),
        silver_version(
            CUSTOMERS,
            8,
            40,
            customer_name="Bob",
            _change_op="delete",
            _changed_at=at(40),
            _is_deleted=True,
        ),
    ]


CUSTOMER_VIEW = [
    {
        "customer_key": 1,
        "customer_id": 7,
        "customer_name": "Ada",
        "_valid_from": at(10),
        "_valid_to": at(30),
        "_is_current": False,
    },
    {
        "customer_key": 2,
        "customer_id": 7,
        "customer_name": "Ada L.",
        "_valid_from": at(30),
        "_valid_to": None,
        "_is_current": True,
    },
    {
        "customer_key": 3,
        "customer_id": 8,
        "customer_name": "Bob",
        "_valid_from": at(11),
        "_valid_to": at(40),
        "_is_current": False,
    },
]


def order_changes() -> list[Row]:
    """Order 70 is re-snapshotted unchanged and then deleted; order 71 is re-snapshotted."""
    values = {"customer_id": 5, "status": "PENDING"}
    return [
        silver_version(ORDERS, 70, 1, _changed_at=at(1), _content_hash="x", **values),
        silver_version(
            ORDERS, 70, 2, _change_op="snapshot", _changed_at=at(2), _content_hash="x", **values
        ),
        silver_version(
            ORDERS,
            70,
            3,
            _change_op="delete",
            _changed_at=at(3),
            _content_hash="x",
            _is_deleted=True,
            **values,
        ),
        silver_version(ORDERS, 71, 4, _changed_at=at(4), _content_hash="y", **values),
        silver_version(
            ORDERS, 71, 5, _change_op="snapshot", _changed_at=at(5), _content_hash="y", **values
        ),
    ]


def order_change(order: int, second: int, op: str, valid_to: int | None) -> dict[str, object]:
    return {
        "order_id": order,
        "_changed_at": at(second),
        "_change_op": op,
        "status_key": 1,
        "customer_id": 5,
        "customer_key": 9,
        "_valid_to": None if valid_to is None else at(valid_to),
        "_is_current": valid_to is None and op != "delete",
    }
