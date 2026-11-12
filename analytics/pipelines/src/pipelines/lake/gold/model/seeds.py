from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from pipelines.lake.shared.types import SPARK_TYPES
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

ORDER_STATUSES = (
    (1, "PENDING", True),
    (2, "PROCESSING", True),
    (3, "REPROCESSING", True),
    (4, "COMPLETED", False),
    (5, "UNKNOWN", True),
)
FIRST_DAY = date(2000, 1, 1)
LAST_DAY = date(2099, 12, 31)
CALENDAR_DATE = "calendar_date"


def table_schema(table: GoldTable) -> StructType:
    """The Spark schema of a gold table's columns."""
    return StructType(
        [
            StructField(column.name, SPARK_TYPES[column.type], nullable=not column.not_null)
            for column in table.columns
        ]
    )


def order_statuses(spark: SparkSession) -> DataFrame:
    """The rows of dim_order_status: every status is open except COMPLETED."""
    return spark.createDataFrame(
        ORDER_STATUSES, table_schema(load_gold_tables()["dim_order_status"])
    )


def calendar_days(spark: SparkSession, first: date = FIRST_DAY, last: date = LAST_DAY) -> DataFrame:
    """The rows of dim_date, one per day from `first` to `last`."""
    day = F.col(CALENDAR_DATE)
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(first), F.lit(last))).alias(CALENDAR_DATE)
    )
    return days.select(
        F.date_format(day, "yyyyMMdd").cast("int").alias("date_key"),
        day,
        F.year(day).alias("year"),
        F.quarter(day).alias("quarter"),
        F.month(day).alias("month"),
        F.dayofmonth(day).alias("day_of_month"),
        (F.weekday(day) + 1).alias("day_of_week"),
        (F.weekday(day) >= 5).alias("is_weekend"),
    )
