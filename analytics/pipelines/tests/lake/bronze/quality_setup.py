from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.bronze.quality import COORDINATES, Committed
from pipelines.lake.shared.checkpoints import CommittedBatch

TOPIC = "test.operations.customers"


def coordinates(spark: SparkSession, *offsets: int, topic: str = TOPIC) -> DataFrame:
    return spark.createDataFrame([(topic, 0, offset) for offset in offsets], COORDINATES)


def committed(
    spark: SparkSession, *, landed: tuple[int, ...], held: tuple[int, ...], batch: int = 0
) -> Committed:
    """A query that committed `batch`, after reading the records at `landed` and writing those at
    `held`."""
    return Committed(
        landed=coordinates(spark, *landed),
        held=coordinates(spark, *held),
        batch=CommittedBatch(query_id="query", batch=batch, sources=()),
    )
