from functools import reduce

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.quality.checks import CheckResult
from pipelines.settings import KafkaSettings, LandingSettings

RECORD_SCHEMA = StructType(
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
PARTITION = ["topic", "partition"]


def read_landing(spark: SparkSession, settings: LandingSettings) -> DataFrame:
    """Read every topic folder through Spark, so only files committed by the file sink count."""
    folders = [read_folder(spark, settings.landing_path(topic)) for topic in settings.kafka.topics]
    return reduce(DataFrame.unionByName, folders)


def read_folder(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.format("avro").load(path).select(*RECORD_SCHEMA.fieldNames())
    except AnalysisException:
        return spark.createDataFrame([], RECORD_SCHEMA)


def read_kafka(spark: SparkSession, kafka: KafkaSettings) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", ",".join(kafka.topics))
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(*RECORD_SCHEMA.fieldNames())
    )


def reconcile(
    landed: DataFrame, kafka: DataFrame, *, last_offsets: int | None = None
) -> list[CheckResult]:
    """Compare landed records with Kafka per topic partition.

    With ``last_offsets``, only the latest landed offsets of each partition are compared, and
    the start of the partition is not checked.
    """
    compared_kafka = kafka
    if last_offsets is not None:
        bounds = landed.groupBy(*PARTITION).agg(F.max("offset").alias("window_end"))
        in_window = F.col("offset") > (F.col("window_end") - last_offsets)
        landed = landed.join(bounds, PARTITION).where(in_window).drop("window_end")
        compared_kafka = (
            kafka.join(bounds, PARTITION)
            .where(in_window & (F.col("offset") <= F.col("window_end")))
            .drop("window_end")
        )

    landed_stats = landed.groupBy(*PARTITION).agg(
        F.count("*").alias("rows"),
        F.countDistinct("offset").alias("distinct_offsets"),
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
        F.sum(F.col("value").isNull().cast("int")).alias("tombstones"),
    )
    kafka_stats = kafka.groupBy(*PARTITION).agg(
        F.min("offset").alias("kafka_earliest"), F.max("offset").alias("kafka_latest")
    )
    matched = (
        landed.alias("l")
        .join(compared_kafka.alias("k"), [*PARTITION, "offset"], "left")
        .join(kafka_stats, PARTITION, "left")
    )
    present = F.col("k.timestamp").isNotNull()
    same_bytes = F.col("l.key").eqNullSafe(F.col("k.key")) & F.col("l.value").eqNullSafe(
        F.col("k.value")
    )
    retained = F.col("offset") >= F.col("kafka_earliest")
    comparison = matched.groupBy(*PARTITION).agg(
        F.sum(present.cast("int")).alias("compared"),
        F.sum((present & ~same_bytes).cast("int")).alias("different"),
        F.sum((~present & retained).cast("int")).alias("missing_in_kafka"),
    )
    partitions = (
        landed_stats.join(kafka_stats, PARTITION, "full")
        .join(comparison, PARTITION, "left")
        .orderBy(*PARTITION)
        .collect()
    )
    return [
        result
        for partition in partitions
        for result in partition_results(partition, check_start=last_offsets is None)
    ]


def partition_results(stats: Row, *, check_start: bool) -> list[CheckResult]:
    subject = f"{stats.topic}[{stats.partition}]"
    if not stats.rows:
        return [
            CheckResult(
                check="landed",
                subject=subject,
                passed=False,
                metrics={
                    "rows": 0,
                    "kafka_earliest": stats.kafka_earliest,
                    "kafka_latest": stats.kafka_latest,
                },
            )
        ]

    lag = None if stats.kafka_latest is None else stats.kafka_latest - stats.max_offset
    expected_offsets = stats.max_offset - stats.min_offset + 1
    results = [
        CheckResult(
            check="landed",
            subject=subject,
            passed=True,
            metrics={
                "rows": stats.rows,
                "min_offset": stats.min_offset,
                "max_offset": stats.max_offset,
                "tombstones": stats.tombstones,
                "kafka_earliest": stats.kafka_earliest,
                "kafka_latest": stats.kafka_latest,
                "lag": lag,
            },
        ),
        CheckResult(
            check="unique",
            subject=subject,
            passed=stats.rows == stats.distinct_offsets,
            metrics={"rows": stats.rows, "distinct_offsets": stats.distinct_offsets},
        ),
        CheckResult(
            check="contiguous",
            subject=subject,
            passed=stats.distinct_offsets == expected_offsets,
            metrics={
                "distinct_offsets": stats.distinct_offsets,
                "expected_offsets": expected_offsets,
            },
        ),
    ]
    if check_start and stats.kafka_earliest is not None:
        results.append(
            CheckResult(
                check="complete_from_start",
                subject=subject,
                passed=stats.min_offset <= stats.kafka_earliest,
                metrics={"min_offset": stats.min_offset, "kafka_earliest": stats.kafka_earliest},
            )
        )
    results.append(
        CheckResult(
            check="identical",
            subject=subject,
            passed=stats.different == 0 and stats.missing_in_kafka == 0,
            metrics={
                "compared": stats.compared,
                "different": stats.different,
                "missing_in_kafka": stats.missing_in_kafka,
            },
        )
    )
    return results
