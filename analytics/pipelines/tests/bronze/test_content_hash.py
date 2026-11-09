import hashlib
from collections.abc import Callable, Iterator
from datetime import date, datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.bronze.content_hash_samples import COLUMNS, content, frame, hashes
from pipelines.bronze.content_hash import UnsupportedHashTypeError, content_hash

TIME_ZONE = "spark.sql.session.timeZone"


@pytest.fixture
def set_time_zone(spark: SparkSession) -> Iterator[Callable[[str], None]]:
    previous = spark.conf.get(TIME_ZONE)
    assert previous is not None
    try:
        yield lambda zone: spark.conf.set(TIME_ZONE, zone)
    finally:
        spark.conf.set(TIME_ZONE, previous)


def test_the_hash_covers_sorted_length_prefixed_non_null_values(spark: SparkSession) -> None:
    canonical = "|".join(
        [
            "active:4:true",
            "changed:24:2026-11-08T14:21:00.022Z",
            "name:8:ACME INC",
            "owner_id:2:42",
            "placed_on:10:2026-11-18",
            "price:12:1.5300000000",
            "units:1:3",
        ]
    )

    assert hashes(frame(spark, content())) == [hashlib.sha256(canonical.encode()).hexdigest()]


def test_equal_content_has_an_equal_hash(spark: SparkSession) -> None:
    first, second = hashes(frame(spark, content(), content()))

    assert first == second


@pytest.mark.parametrize(
    "change",
    [
        {"name": "ACME LLC"},
        {"active": False},
        {"units": 4},
        {"owner_id": 43},
        {"price": Decimal("1.54")},
        {"placed_on": date(2026, 11, 19)},
        {"changed": datetime(2026, 11, 8, 14, 21, 0, 23000)},
        {"note": "fragile"},
    ],
    ids=lambda change: next(iter(change)),
)
def test_any_content_change_changes_the_hash(
    spark: SparkSession, change: dict[str, object]
) -> None:
    before, after = hashes(frame(spark, content(), content(**change)))

    assert before != after


def test_null_and_empty_string_differ(spark: SparkSession) -> None:
    null, empty = hashes(frame(spark, content(note=None), content(note="")))

    assert null != empty


def test_values_cannot_shift_between_columns(spark: SparkSession) -> None:
    first, second = hashes(
        frame(spark, content(name="a|b", note=None), content(name="a", note="b"))
    )

    assert first != second


def test_adding_a_column_that_is_still_null_keeps_the_hash(spark: SparkSession) -> None:
    records = frame(spark, content(note=None))

    without_note = hashes(records, tuple(name for name in COLUMNS if name != "note"))

    assert hashes(records) == without_note


def test_the_column_order_does_not_matter(spark: SparkSession) -> None:
    records = frame(spark, content())

    assert hashes(records) == hashes(records, tuple(reversed(COLUMNS)))


@pytest.mark.parametrize("zone", ["-10:00", "-03:30", "UTC", "+05:30", "+05:45", "+14:00"])
def test_timestamps_hash_as_in_utc_whatever_the_session_time_zone(
    spark: SparkSession, set_time_zone: Callable[[str], None], zone: str
) -> None:
    records = spark.sql("select timestamp_millis(1794147660022) as changed")
    set_time_zone("UTC")
    in_utc = hashes(records, ("changed",))

    set_time_zone(zone)

    assert hashes(records, ("changed",)) == in_utc


def test_unsupported_types_are_rejected(spark: SparkSession) -> None:
    records = spark.createDataFrame([(1.5,)], "ratio double")

    with pytest.raises(UnsupportedHashTypeError, match="'ratio' of type double"):
        content_hash(records.schema, ("ratio",))
