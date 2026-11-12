from collections.abc import Callable, Sequence
from functools import reduce

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructType

from pipelines.quality.checks import Check, CheckResult
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingQualitySettings, LandingSettings

PARTITION = ["topic", "partition"]

TOPIC_FOUND = Check(
    "topic_found", "A configured topic has records in Kafka or in the landing zone."
)
PARTITION_LANDED = Check("partition_landed", "Every Kafka partition has landed records.")
UNIQUE_OFFSETS = Check("unique_offsets", "Every offset landed exactly once.")
CONTIGUOUS_OFFSETS = Check(
    "contiguous_offsets", "Every offset Kafka holds in the compared range landed."
)
STARTS_AT_KAFKA_MIN_OFFSET = Check(
    "starts_at_kafka_min_offset",
    "Landing starts at or before Kafka's earliest retained offset.",
)
SAME_BYTES_AS_KAFKA = Check(
    "same_bytes_as_kafka",
    "Landed keys and values equal Kafka's, and Kafka holds every landed offset it still retains.",
)


class LandingSchemaError(ValueError):
    def __init__(self, path: str, schema: StructType) -> None:
        super().__init__(
            f"{path} does not hold Kafka records: expected {KAFKA_RECORD_SCHEMA.simpleString()},"
            f" found {schema.simpleString()}"
        )


def read_landing(spark: SparkSession, settings: LandingSettings) -> DataFrame:
    """Read every topic folder through Spark, so only files committed by the file sink count."""
    folders = [read_folder(spark, settings.landing_path(topic)) for topic in settings.kafka.topics]
    return reduce(DataFrame.unionByName, folders)


def read_folder(spark: SparkSession, path: str) -> DataFrame:
    if not committed_files(spark, path):
        return spark.createDataFrame([], KAFKA_RECORD_SCHEMA)
    records = spark.read.format("avro").load(path)
    if column_types(records.schema) != column_types(KAFKA_RECORD_SCHEMA):
        raise LandingSchemaError(path, records.schema)
    return records.select(*KAFKA_RECORD_SCHEMA.fieldNames())


def committed_files(spark: SparkSession, path: str) -> list[str]:
    """List the data files a reader of ``path`` sees: with a file sink log, the committed ones.

    Avro schema inference fails on a folder without data files, so the files are listed with
    the expected schema, which needs no inference. A missing folder holds no files.
    """
    try:
        return spark.read.format("avro").schema(KAFKA_RECORD_SCHEMA).load(path).inputFiles()
    except AnalysisException as error:
        if error.getCondition() == "PATH_NOT_FOUND":
            return []
        raise


def column_types(schema: StructType) -> dict[str, DataType]:
    return {field.name: field.dataType for field in schema.fields}


def read_kafka(spark: SparkSession, kafka: KafkaSettings) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", ",".join(kafka.topics))
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(*KAFKA_RECORD_SCHEMA.fieldNames())
    )


def reconcile(
    landed: DataFrame,
    kafka: DataFrame,
    *,
    topics: Sequence[str],
    last_offsets: int | None = None,
) -> list[CheckResult]:
    """Compare landed records with Kafka per topic partition.

    Each landed partition is compared over a range of offsets: from its first to its last landed
    offset or, with ``last_offsets``, only the last ``last_offsets`` offsets up to its last landed
    one. Kafka is the reference: an offset counts as missing only if Kafka holds a record there,
    and Kafka offsets after the last landed one are lag. Where landing started is checked only
    when the whole partition is compared.
    """
    ranges = compared_ranges(landed, last_offsets)
    compared = (
        landed.alias("l")
        .join(ranges.alias("b"), same_partition("l", "b"))
        .where(F.col("l.offset") >= F.col("b.from_offset"))
        .select("l.*")
    )
    kafka_bounds = kafka.groupBy(*PARTITION).agg(
        F.min("offset").alias("kafka_min_offset"),
        F.max("offset").alias("kafka_max_offset"),
    )
    partitions = (
        landed_stats(compared)
        .join(kafka_bounds, PARTITION, "full")
        .join(ranges, PARTITION, "left")
        .join(missing_offsets(compared, kafka, ranges), PARTITION, "left")
        .withColumn("missing_offsets", F.coalesce(F.col("missing_offsets"), F.lit(0)))
        .join(byte_comparison(compared, kafka, kafka_bounds), PARTITION, "left")
        .orderBy(*PARTITION)
        .collect()
    )
    found = {partition.topic for partition in partitions}
    return [
        *(
            result
            for partition in partitions
            for result in partition_results(partition, check_start=last_offsets is None)
        ),
        *(
            TOPIC_FOUND.result(
                topic, passed=False, metrics={"kafka_partitions": 0, "landed_partitions": 0}
            )
            for topic in topics
            if topic not in found
        ),
    ]


def same_partition(left: str, right: str) -> Column:
    return (F.col(f"{left}.topic") == F.col(f"{right}.topic")) & (
        F.col(f"{left}.partition") == F.col(f"{right}.partition")
    )


def same_record(left: str, right: str) -> Column:
    """Match on topic, partition and offset: the whole key of a Kafka record."""
    return same_partition(left, right) & (F.col(f"{left}.offset") == F.col(f"{right}.offset"))


def count_where(condition: Column) -> Column:
    return F.coalesce(F.sum(condition.cast("int")), F.lit(0))


def compared_ranges(landed: DataFrame, last_offsets: int | None) -> DataFrame:
    ranges = landed.groupBy(*PARTITION).agg(
        F.min("offset").alias("from_offset"),
        F.max("offset").alias("to_offset"),
    )
    if last_offsets is None:
        return ranges
    window_start = F.col("to_offset") - F.lit(last_offsets - 1)
    return ranges.withColumn("from_offset", F.greatest(window_start, F.lit(0)))


def landed_stats(compared: DataFrame) -> DataFrame:
    return compared.groupBy(*PARTITION).agg(
        F.count("*").alias("rows"),
        F.countDistinct("offset").alias("distinct_offsets"),
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
        count_where(F.col("value").isNull()).alias("tombstones"),
    )


def missing_offsets(compared: DataFrame, kafka: DataFrame, ranges: DataFrame) -> DataFrame:
    """Count the records Kafka holds in each compared range that did not land."""
    kafka_in_range = (
        kafka.alias("k")
        .join(ranges.alias("b"), same_partition("k", "b"))
        .where(F.col("k.offset").between(F.col("b.from_offset"), F.col("b.to_offset")))
        .select("k.*")
    )
    return (
        kafka_in_range.alias("k")
        .join(compared.alias("l"), same_record("k", "l"), "left_anti")
        .groupBy(*PARTITION)
        .agg(F.count("*").alias("missing_offsets"))
    )


def byte_comparison(compared: DataFrame, kafka: DataFrame, kafka_bounds: DataFrame) -> DataFrame:
    # An unmatched row of the left join has null key columns; a Kafka record never does.
    in_kafka = F.col("k.offset").isNotNull()
    same_key = F.col("l.key").eqNullSafe(F.col("k.key"))
    same_value = F.col("l.value").eqNullSafe(F.col("k.value"))
    # A partition whose records all expired has no Kafka bounds, so nothing in it is retained.
    retained = F.coalesce(F.col("l.offset") >= F.col("kb.kafka_min_offset"), F.lit(False))
    return (
        compared.alias("l")
        .join(kafka.alias("k"), same_record("l", "k"), "left")
        .join(kafka_bounds.alias("kb"), same_partition("l", "kb"), "left")
        .groupBy(F.col("l.topic").alias("topic"), F.col("l.partition").alias("partition"))
        .agg(
            count_where(in_kafka).alias("compared"),
            count_where(in_kafka & ~(same_key & same_value)).alias("different"),
            count_where((~in_kafka) & retained).alias("missing_in_kafka"),
        )
    )


def partition_results(stats: Row, *, check_start: bool) -> list[CheckResult]:
    subject = f"{stats.topic}[{stats.partition}]"
    kafka_range = {
        "kafka_min_offset": stats.kafka_min_offset,
        "kafka_max_offset": stats.kafka_max_offset,
    }
    if not stats.rows:
        return [PARTITION_LANDED.result(subject, passed=False, metrics={"rows": 0, **kafka_range})]

    lag = None if stats.kafka_max_offset is None else stats.kafka_max_offset - stats.max_offset
    results = [
        PARTITION_LANDED.result(
            subject,
            passed=True,
            metrics={
                "rows": stats.rows,
                "min_offset": stats.min_offset,
                "max_offset": stats.max_offset,
                "tombstones": stats.tombstones,
                **kafka_range,
                "lag": lag,
            },
        ),
        UNIQUE_OFFSETS.result(
            subject,
            passed=stats.rows == stats.distinct_offsets,
            metrics={"rows": stats.rows, "distinct_offsets": stats.distinct_offsets},
        ),
        CONTIGUOUS_OFFSETS.result(
            subject,
            passed=stats.missing_offsets == 0,
            metrics={
                "from_offset": stats.from_offset,
                "to_offset": stats.to_offset,
                "missing_offsets": stats.missing_offsets,
            },
        ),
    ]
    if check_start and stats.kafka_min_offset is not None:
        results.append(
            STARTS_AT_KAFKA_MIN_OFFSET.result(
                subject,
                passed=stats.min_offset <= stats.kafka_min_offset,
                metrics={
                    "min_offset": stats.min_offset,
                    "kafka_min_offset": stats.kafka_min_offset,
                },
            )
        )
    results.append(
        SAME_BYTES_AS_KAFKA.result(
            subject,
            passed=(stats.different == 0) and (stats.missing_in_kafka == 0),
            metrics={
                "compared": stats.compared,
                "different": stats.different,
                "missing_in_kafka": stats.missing_in_kafka,
            },
        )
    )
    return results


type LandingReader = Callable[[SparkSession, LandingSettings], DataFrame]
type RecordReader = Callable[[SparkSession, KafkaSettings], DataFrame]


def run_checks(
    spark: SparkSession,
    settings: LandingQualitySettings,
    *,
    read_landed: LandingReader = read_landing,
    read_records: RecordReader = read_kafka,
) -> list[CheckResult]:
    landed = read_landed(spark, settings.landing)
    # Kafka keeps receiving records while the checks run; one materialised snapshot keeps the
    # partition bounds and the record comparison consistent with each other.
    kafka = read_records(spark, settings.landing.kafka).localCheckpoint(eager=True)
    return reconcile(
        landed,
        kafka,
        topics=settings.landing.kafka.topics,
        last_offsets=settings.last_offsets,
    )
