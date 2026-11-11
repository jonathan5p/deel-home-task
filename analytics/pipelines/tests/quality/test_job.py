import json
from collections.abc import Mapping
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.quality.report_setup import LAKE_ENV, result
from pipelines.quality.checks import CheckResult
from pipelines.quality.job import CHECKS, run, selected_checks
from pipelines.quality.report import Results
from pipelines.settings import LakeSettings


def test_all_or_no_name_selects_every_check_in_registry_order() -> None:
    assert selected_checks([], CHECKS) == ["landing", "bronze", "silver", "gold"]
    assert selected_checks(["all"], CHECKS) == ["landing", "bronze", "silver", "gold"]
    assert selected_checks(["bronze", "landing"], CHECKS) == ["landing", "bronze"]
    assert selected_checks(["bronze"], CHECKS) == ["bronze"]
    assert selected_checks(["gold", "landing"], CHECKS) == ["landing", "gold"]
    assert selected_checks(["gold", "silver"], CHECKS) == ["silver", "gold"]


def test_an_unknown_check_is_refused() -> None:
    with pytest.raises(SystemExit):
        selected_checks(["platinum"], CHECKS)


def test_a_run_prints_each_check_then_the_run_summary_and_writes_one_report(
    spark: SparkSession,
) -> None:
    lines: list[str] = []
    reports: list[Results] = []

    def passing(_: SparkSession, env: Mapping[str, str]) -> list[CheckResult]:
        assert env == LAKE_ENV
        return [result("a", passed=True)]

    def failing(_: SparkSession, __: Mapping[str, str]) -> list[CheckResult]:
        return [result("b", passed=False)]

    def write(_: SparkSession, lake: LakeSettings, results: Results) -> str:
        reports.append(results)
        return f"{lake.root}/quality/reports/run.json"

    passed = run(
        spark,
        LAKE_ENV,
        ["landing", "bronze"],
        checks={"landing": passing, "bronze": failing},
        write=write,
        out=lines.append,
    )

    assert not passed
    assert [line.split(" {")[0] for line in lines] == [
        "LANDING QUALITY",
        "LANDING QUALITY",
        "BRONZE QUALITY",
        "BRONZE QUALITY",
        "DATA QUALITY",
    ]
    summary = json.loads(lines[-1].removeprefix("DATA QUALITY "))
    assert summary == {
        "summary": True,
        "checks": 2,
        "failed": 1,
        "passed": False,
        "report": "s3a://lake/quality/reports/run.json",
    }
    assert [list(results) for results in reports] == [["landing", "bronze"]]


def test_the_bronze_check_builds_its_settings_from_the_environment(
    spark: SparkSession, tmp_path: Path
) -> None:
    env = {
        "KAFKA_TOPICS": "test.operations.customers",
        "LAKE_ROOT": f"file://{tmp_path}/lake",
        "ICEBERG_CATALOG": "lake",
    }

    results = CHECKS["bronze"](spark, env)

    assert {result.subject for result in results} == {"test.operations.customers"}
