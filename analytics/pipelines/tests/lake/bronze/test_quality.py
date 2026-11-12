import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.bronze.job_setup import (
    BronzeSettingsFactory,
    land,
    record,
    valid,
)
from analytics.pipelines.tests.lake.bronze.parse_samples import topic
from analytics.pipelines.tests.lake.bronze.quality_setup import TOPIC, committed
from pipelines.lake.bronze.entities import CUSTOMERS
from pipelines.lake.bronze.job import run
from pipelines.lake.bronze.quality import reconcile, run_checks
from pipelines.lake.shared.columns import MetadataColumn
from pipelines.lake.shared.streams import await_queries
from pipelines.quality.checks import CheckResult


def by_check(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {result.check: result for result in results}


def test_records_split_between_bronze_and_quarantine_pass(spark: SparkSession) -> None:
    bronze = committed(spark, landed=(0, 1, 2), held=(0, 2))
    quarantine = committed(spark, landed=(0, 1, 2), held=(1,))

    results = by_check(reconcile(TOPIC, bronze, quarantine))

    assert all(result.passed for result in results.values())
    assert {result.subject for result in results.values()} == {TOPIC}
    assert results["landed_records_held"].metrics["committed_records"] == 3
    assert results["held_exactly_once"].metrics["held_records"] == 3


def test_a_record_in_neither_table_is_missing(spark: SparkSession) -> None:
    bronze = committed(spark, landed=(0, 1, 2), held=(0,))
    quarantine = committed(spark, landed=(0, 1, 2), held=(1,))

    result = by_check(reconcile(TOPIC, bronze, quarantine))["landed_records_held"]

    assert not result.passed
    assert result.metrics["missing"] == 1


def test_a_record_only_one_query_committed_is_not_missing_yet(spark: SparkSession) -> None:
    bronze = committed(spark, landed=(0, 1, 2, 3), held=(0, 2))
    quarantine = committed(spark, landed=(0, 1, 2), held=(1,))

    results = reconcile(TOPIC, bronze, quarantine)

    assert all(result.passed for result in results)


@pytest.mark.parametrize(
    ("bronze_held", "quarantine_held"),
    [((0, 1, 2), (1,)), ((0, 0, 2), (1,))],
    ids=["in both tables", "twice in bronze"],
)
def test_a_record_held_twice_is_a_duplicate(
    spark: SparkSession, bronze_held: tuple[int, ...], quarantine_held: tuple[int, ...]
) -> None:
    bronze = committed(spark, landed=(0, 1, 2), held=bronze_held)
    quarantine = committed(spark, landed=(0, 1, 2), held=quarantine_held)

    result = by_check(reconcile(TOPIC, bronze, quarantine))["held_exactly_once"]

    assert not result.passed
    assert result.metrics["duplicated"] == 1


def test_a_record_from_a_file_its_query_never_committed_is_unexpected(
    spark: SparkSession,
) -> None:
    bronze = committed(spark, landed=(0, 1), held=(0, 5))
    quarantine = committed(spark, landed=(0, 1), held=(1, 6))

    result = by_check(reconcile(TOPIC, bronze, quarantine))["committed_records_only"]

    assert not result.passed
    assert result.metrics["unexpected"] == 2


def test_the_checks_pass_on_what_the_bronze_job_committed_and_fail_once_a_row_is_lost(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory, iceberg_catalog: str
) -> None:
    settings = bronze_settings(topic(CUSTOMERS), catalog=iceberg_catalog)
    tombstone = record(CUSTOMERS, 2, None)
    rows = [valid(CUSTOMERS, 0), record(CUSTOMERS, 1, "not json"), tombstone, valid(CUSTOMERS, 3)]
    land(spark, settings, rows, "batch-1")
    await_queries(spark.streams, run(spark, settings, available_now=True), available_now=True)

    clean = by_check(run_checks(spark, settings))
    spark.sql(f"DELETE FROM {settings.table('customers')} WHERE {MetadataColumn.SOURCE_OFFSET} = 0")
    lost = by_check(run_checks(spark, settings))

    assert all(result.passed for result in clean.values())
    assert clean["landed_records_held"].metrics == {
        "committed_records": 3,
        "missing": 0,
        "bronze_batch": 0,
        "quarantine_batch": 0,
    }
    assert lost["landed_records_held"].metrics["missing"] == 1


def test_a_topic_bronze_has_not_committed_yet_has_nothing_to_check(
    spark: SparkSession, bronze_settings: BronzeSettingsFactory
) -> None:
    settings = bronze_settings(topic(CUSTOMERS))

    results = by_check(run_checks(spark, settings))

    assert all(result.passed for result in results.values())
    assert results["landed_records_held"].metrics["bronze_batch"] is None
