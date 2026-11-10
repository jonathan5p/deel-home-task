from datetime import datetime

from pyspark.sql import DataFrame, Row, SparkSession

from pipelines.schemas import KAFKA_RECORD_SCHEMA

ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
    "KAFKA_TOPICS": "topic-a",
    "LAKE_ROOT": "s3a://bucket",
    "ICEBERG_CATALOG": "catalog",
}


def records(spark: SparkSession, offsets: range) -> DataFrame:
    rows = [
        Row(
            key=b"k",
            value=b"v",
            topic="topic-a",
            partition=0,
            offset=offset,
            timestamp=datetime(2026, 11, 8, 14, 21),  # Spark returns naive timestamps in UTC
            timestampType=0,
        )
        for offset in offsets
    ]
    return spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA)
