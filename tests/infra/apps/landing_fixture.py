import json
import sys
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LANDING_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
    ]
)
TOPIC_PREFIX = "finance_db.operations."


def event(op: str, before: dict[str, object] | None, after: dict[str, object] | None) -> bytes:
    return json.dumps({"before": before, "after": after, "op": op, "ts_ms": 0}).encode()


def records(now: datetime) -> list[tuple[object, ...]]:
    def record(
        table: str, offset: int, key: dict[str, int], value: bytes | None
    ) -> tuple[object, ...]:
        published = now - timedelta(minutes=10 - offset)
        return (json.dumps(key).encode(), value, TOPIC_PREFIX + table, 0, offset, published, 0)

    pending = {"order_id": 1, "status": "PENDING"}
    shipped = {"order_id": 1, "status": "SHIPPED"}
    second = {"order_id": 2, "status": "PENDING"}
    return [
        record("orders", 0, {"order_id": 1}, event("c", None, pending)),
        record("orders", 1, {"order_id": 2}, event("c", None, second)),
        record("orders", 2, {"order_id": 1}, event("u", pending, shipped)),
        record("orders", 3, {"order_id": 2}, event("d", second, None)),
        record("orders", 4, {"order_id": 2}, None),
        record("customers", 0, {"customer_id": 1}, event("r", None, {"customer_id": 1})),
        record("products", 0, {"product_id": 1}, event("r", None, {"product_id": 1})),
        record("order_items", 0, {"order_item_id": 1}, event("r", None, {"order_item_id": 1})),
    ]


def main(argv: list[str]) -> None:
    root = argv[0]
    spark = SparkSession.builder.appName("LandingFixture").getOrCreate()
    frame = spark.createDataFrame(records(datetime.now(UTC)), LANDING_SCHEMA)
    for row in frame.select("topic").distinct().collect():
        topic: str = row["topic"]
        frame.where(frame.topic == topic).coalesce(1).write.mode("overwrite").format("avro").save(
            f"{root}/{topic}"
        )
    print(f"LANDING_FIXTURE written to {root}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
