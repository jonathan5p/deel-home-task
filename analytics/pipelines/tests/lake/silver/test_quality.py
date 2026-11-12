import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.silver.deduplication_setup import redelivered
from analytics.pipelines.tests.lake.silver.job_setup import (
    SilverSettingsFactory,
    run_until_caught_up,
    write_bronze,
)
from analytics.pipelines.tests.lake.silver.quality_setup import (
    TOPIC,
    committed,
    uncommit_last_batch,
    without_snapshots_of,
)
from analytics.pipelines.tests.lake.silver.versions_samples import bronze_change
from pipelines.lake.bronze.entities import CUSTOMERS
from pipelines.lake.shared.columns import ChangeOp, MetadataColumn
from pipelines.lake.silver.quality import reconcile, run_checks
from pipelines.quality.checks import CheckResult

pytestmark = pytest.mark.usefixtures("job_session_conf")

CREATED = (1, 1, "c")
UPDATED = (1, 2, "u")
DELETED = (1, 3, "d")


def by_check(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {result.check: result for result in results}


def test_silver_holding_bronzes_changes_once_passes(spark: SparkSession) -> None:
    silver = committed(
        spark, bronze=(CREATED, UPDATED, DELETED), silver=(CREATED, UPDATED, DELETED)
    )

    results = by_check(reconcile(TOPIC, silver))

    assert all(result.passed for result in results.values())
    assert {result.subject for result in results.values()} == {TOPIC}
    assert results["same_changes_as_bronze"].metrics == {
        "bronze_changes": 3,
        "missing": 0,
        "extra": 0,
        "silver_batch": 0,
    }
    assert results["same_changes_as_bronze"].details == {"bronze_snapshot": 1, "silver_snapshot": 2}
    assert results["changes_held_once"].metrics == {"silver_rows": 3, "duplicated": 0}


def test_a_change_bronze_holds_twice_passes_once_in_silver(spark: SparkSession) -> None:
    silver = committed(spark, bronze=(CREATED, CREATED, UPDATED), silver=(CREATED, UPDATED))

    results = by_check(reconcile(TOPIC, silver))

    assert all(result.passed for result in results.values())
    assert results["same_changes_as_bronze"].metrics["bronze_changes"] == 2


def test_a_change_silver_lacks_is_missing(spark: SparkSession) -> None:
    silver = committed(spark, bronze=(CREATED, UPDATED), silver=(CREATED,))

    result = by_check(reconcile(TOPIC, silver))["same_changes_as_bronze"]

    assert not result.passed
    assert result.metrics["missing"] == 1
    assert result.details["missing"] == (
        {"key": 1, MetadataColumn.COMMIT_SEQUENCE: 2, MetadataColumn.CHANGE_OP: "u"},
    )


def test_a_change_bronze_never_held_is_extra(spark: SparkSession) -> None:
    silver = committed(spark, bronze=(CREATED,), silver=(CREATED, UPDATED))

    result = by_check(reconcile(TOPIC, silver))["same_changes_as_bronze"]

    assert not result.passed
    assert result.metrics["extra"] == 1
    assert "missing" not in result.details


def test_a_change_silver_holds_twice_is_a_duplicate(spark: SparkSession) -> None:
    silver = committed(spark, bronze=(CREATED, UPDATED), silver=(CREATED, UPDATED, UPDATED))

    results = by_check(reconcile(TOPIC, silver))

    assert results["same_changes_as_bronze"].passed
    assert not results["changes_held_once"].passed
    assert results["changes_held_once"].metrics == {"silver_rows": 3, "duplicated": 1}
    assert results["changes_held_once"].details == {
        "duplicated": (
            {"key": 1, MetadataColumn.COMMIT_SEQUENCE: 2, MetadataColumn.CHANGE_OP: "u"},
        )
    }


def test_the_checks_pass_on_what_the_silver_job_committed_and_fail_once_a_change_is_lost(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    created = bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)
    write_bronze(
        spark,
        settings,
        CUSTOMERS,
        [created, redelivered(created, 50), bronze_change(CUSTOMERS, 2, ChangeOp.UPDATE)],
    )
    run_until_caught_up(spark, settings)

    clean = by_check(run_checks(spark, settings))
    spark.sql(
        f"DELETE FROM {settings.table('customers')} WHERE {MetadataColumn.COMMIT_SEQUENCE} = 1"
    )
    lost = by_check(run_checks(spark, settings))

    assert all(result.passed for result in clean.values())
    assert clean["same_changes_as_bronze"].metrics == {
        "bronze_changes": 2,
        "missing": 0,
        "extra": 0,
        "silver_batch": 0,
    }
    assert lost["same_changes_as_bronze"].metrics["missing"] == 1


def test_a_batch_silver_wrote_but_did_not_commit_is_not_checked_yet(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)])
    run_until_caught_up(spark, settings)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 2, ChangeOp.UPDATE)])
    run_until_caught_up(spark, settings)
    uncommit_last_batch(settings)

    results = by_check(run_checks(spark, settings))

    assert all(result.passed for result in results.values())
    assert results["same_changes_as_bronze"].metrics["silver_batch"] == 0
    assert results["same_changes_as_bronze"].metrics["bronze_changes"] == 1
    assert results["changes_held_once"].metrics["silver_rows"] == 1


def test_an_expired_bronze_snapshot_fails_with_the_reason(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)])
    run_until_caught_up(spark, settings)
    bronze = settings.source_table(CUSTOMERS.name)

    results = by_check(run_checks(spark, settings, snapshots=without_snapshots_of(bronze)))

    result = results["same_changes_as_bronze"]
    assert not result.passed
    assert result.metrics["missing"] is None
    assert "no longer exists" in str(result.details["reason"])
    assert results["changes_held_once"].passed


def test_an_entity_silver_has_not_committed_yet_has_nothing_to_check(
    spark: SparkSession, silver_settings: SilverSettingsFactory
) -> None:
    results = by_check(run_checks(spark, silver_settings(CUSTOMERS)))

    assert all(result.passed for result in results.values())
    assert results["same_changes_as_bronze"].metrics["silver_batch"] is None
