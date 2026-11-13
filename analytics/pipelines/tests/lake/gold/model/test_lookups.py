from datetime import date

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.model.model_samples import (
    ORDERS,
    order,
    rows_of,
    seeded,
    staged,
)
from pipelines.lake.gold.model.lookups import with_key_lookups


def test_a_status_takes_its_key_and_an_unknown_or_missing_one_the_unknown_status(
    spark: SparkSession,
) -> None:
    orders = staged(
        spark,
        ORDERS,
        [order(1, 1, "PENDING"), order(2, 2, "COMPLETED"), order(3, 3, "LOST"), order(4, 4, None)],
    )

    looked_up = with_key_lookups(orders, ORDERS.lookups, seeded(spark))

    assert rows_of(looked_up, "order_id", "status_key", "is_open") == [
        (1, 1, True),
        (2, 4, False),
        (3, 5, True),
        (4, 5, True),
    ]


def test_a_date_takes_its_calendar_key_and_a_date_outside_the_calendar_none(
    spark: SparkSession,
) -> None:
    orders = staged(
        spark,
        ORDERS,
        [
            order(1, 1, delivery_date=date(2026, 11, 20)),
            order(2, 2, delivery_date=date(1999, 1, 1)),
        ],
    )

    looked_up = with_key_lookups(orders, ORDERS.lookups, seeded(spark))

    assert rows_of(looked_up, "order_id", "order_date_key", "delivery_date_key") == [
        (1, 20261101, 20261120),
        (2, 20261101, None),
    ]
