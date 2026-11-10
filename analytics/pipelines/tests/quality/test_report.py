import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.quality.report_setup import LAKE_ENV, FakeWriter, result
from pipelines.quality.report import hadoop_writer, report_document, report_path, write_report
from pipelines.settings import LakeSettings

LAKE = LakeSettings.from_env(LAKE_ENV)
NOW = datetime(2026, 11, 10, 15, 4, 5, tzinfo=UTC)


def test_the_document_groups_the_results_by_check_with_a_summary_of_each_and_of_the_run() -> None:
    document = report_document(
        {"landing": [result("a", passed=True)], "bronze": [result("b", passed=False)]}
    )

    checks = document["checks"]
    assert isinstance(checks, dict)
    assert checks["landing"]["results"] == [
        {
            "check": "rows_landed",
            "subject": "a",
            "passed": True,
            "metrics": {"missing": 0},
            "description": "Every row landed.",
        }
    ]
    assert checks["landing"]["summary"]["passed"] is True
    assert checks["bronze"]["summary"]["failed"] == 1
    assert document["summary"] == {"summary": True, "checks": 2, "failed": 1, "passed": False}


def test_one_report_per_run_is_named_after_its_utc_time_under_the_lake() -> None:
    bogota = NOW.astimezone(timezone(timedelta(hours=-5)))

    assert report_path(LAKE, bogota) == "s3a://lake/quality/reports/20261110T150405Z.json"


def test_the_report_is_written_as_json_and_its_path_returned(spark: SparkSession) -> None:
    writer = FakeWriter()

    path = write_report(spark, LAKE, {"bronze": [result("b", passed=True)]}, now=NOW, write=writer)

    assert path == "s3a://lake/quality/reports/20261110T150405Z.json"
    assert json.loads(writer.files[path])["summary"]["passed"] is True


def test_the_hadoop_writer_writes_a_file_and_replaces_it(
    spark: SparkSession, tmp_path: Path
) -> None:
    write = hadoop_writer(spark)
    path = tmp_path / "quality" / "report.json"

    write(f"file://{path}", "first")
    write(f"file://{path}", "{}\n")

    assert path.read_text() == "{}\n"
