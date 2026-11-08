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


def event(op: str, before: dict[str, object] | None, after: dict[str, object] | None) -> bytes:
    return json.dumps({"before": before, "after": after, "op": op, "ts_ms": 0}).encode()


def records(now: datetime) -> list[tuple[object, ...]]:
    def record(topic: str, offset: int, key: int, value: bytes | None) -> tuple[object, ...]:
        published = now - timedelta(minutes=10 - offset)
        return (json.dumps({"id": key}).encode(), value, topic, 0, offset, published, 0)

    first = {"id": 1, "state": "a"}
    first_changed = {"id": 1, "state": "b"}
    second = {"id": 2, "state": "a"}
    third = {"id": 3, "state": "a"}
    return [
        record("topic-a", 0, 1, event("c", None, first)),
        record("topic-a", 1, 2, event("c", None, second)),
        record("topic-a", 2, 1, event("u", first, first_changed)),
        record("topic-a", 3, 2, event("d", second, None)),
        record("topic-a", 4, 2, None),
        record("topic-a", 5, 3, event("r", None, third)),
        record("topic-b", 0, 1, event("c", None, first)),
    ]


def main(argv: list[str]) -> None:
    """Write known landing-shaped records under the given root for the landing inspection tests."""
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
