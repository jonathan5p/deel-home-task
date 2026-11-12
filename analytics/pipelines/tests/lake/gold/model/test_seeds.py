from datetime import date

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import rows_of
from pipelines.lake.gold.model.seeds import calendar_days, order_statuses, table_schema
from pipelines.schemas.gold_definitions import load_gold_tables


def test_every_order_status_is_open_except_completed(spark: SparkSession) -> None:
    statuses = order_statuses(spark)

    assert rows_of(statuses, "status_code", "is_open") == [
        ("COMPLETED", False),
        ("PENDING", True),
        ("PROCESSING", True),
        ("REPROCESSING", True),
        ("UNKNOWN", True),
    ]


def test_a_calendar_day_has_its_key_and_iso_weekday(spark: SparkSession) -> None:
    days = calendar_days(spark, date(2026, 11, 8), date(2026, 11, 9))

    assert [tuple(row) for row in days.orderBy("date_key").collect()] == [
        (20261108, date(2026, 11, 8), 2026, 4, 11, 8, 7, True),
        (20261109, date(2026, 11, 9), 2026, 4, 11, 9, 1, False),
    ]


def test_the_calendar_has_the_columns_of_dim_date_and_spans_the_century(
    spark: SparkSession,
) -> None:
    days = calendar_days(spark)
    dim_date = load_gold_tables()["dim_date"]

    assert days.columns == [field.name for field in table_schema(dim_date)]
    assert rows_of(days.groupBy().agg({"date_key": "min"}), "min(date_key)") == [(20000101,)]
    assert rows_of(days.groupBy().agg({"date_key": "max"}), "max(date_key)") == [(20991231,)]
