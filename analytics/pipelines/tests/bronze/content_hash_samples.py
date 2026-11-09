from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

from pipelines.bronze.content_hash import content_hash

SCHEMA = (
    "name string, active boolean, units int, owner_id bigint, price decimal(38,10),"
    " placed_on date, changed timestamp, note string"
)
COLUMNS = ("name", "active", "units", "owner_id", "price", "placed_on", "changed", "note")


def content(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "name": "ACME INC",
        "active": True,
        "units": 3,
        "owner_id": 42,
        "price": Decimal("1.53"),
        "placed_on": date(2026, 11, 18),
        "changed": datetime(2026, 11, 8, 14, 21, 0, 22000),
        "note": None,
    }
    return values | overrides


def frame(spark: SparkSession, *rows: dict[str, object]) -> DataFrame:
    return spark.createDataFrame([tuple(row[name] for name in COLUMNS) for row in rows], SCHEMA)


def hashes(records: DataFrame, columns: tuple[str, ...] = COLUMNS) -> list[str]:
    hashed = records.select(content_hash(records.schema, columns).alias("hash"))
    return [row.hash for row in hashed.collect()]
