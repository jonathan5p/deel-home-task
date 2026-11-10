import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from itertools import chain

from pyspark.sql import SparkSession

from pipelines.quality.checks import CheckResult, result_object, summary_object
from pipelines.settings import LakeSettings

REPORTS = "quality/reports"

type TextWriter = Callable[[str, str], None]
type Results = Mapping[str, Sequence[CheckResult]]


def report_document(results: Results) -> dict[str, object]:
    """Every check's results and summary, by check name, then the summary of the whole run."""
    checks = {
        name: {
            "results": [result_object(result) for result in check_results],
            "summary": summary_object(check_results),
        }
        for name, check_results in results.items()
    }
    return {"checks": checks, "summary": summary_object(list(chain(*results.values())))}


def report_path(lake: LakeSettings, now: datetime) -> str:
    return f"{lake.root}/{REPORTS}/{now.astimezone(UTC):%Y%m%dT%H%M%SZ}.json"


def hadoop_writer(spark: SparkSession) -> TextWriter:
    """Writes through Spark's Hadoop file systems, so s3a:// and file:// both work."""
    jvm = spark.sparkContext._jvm
    java_context = spark.sparkContext._jsc
    assert jvm is not None
    assert java_context is not None
    conf = java_context.hadoopConfiguration()

    def write(path: str, text: str) -> None:
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        stream = hadoop_path.getFileSystem(conf).create(hadoop_path, True)
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()

    return write


def write_report(
    spark: SparkSession,
    lake: LakeSettings,
    results: Results,
    *,
    now: datetime | None = None,
    write: TextWriter | None = None,
) -> str:
    """Write one run's report as JSON under `<LAKE_ROOT>/quality/reports/`; returns its path."""
    path = report_path(lake, now or datetime.now(UTC))
    (write or hadoop_writer(spark))(path, json.dumps(report_document(results), indent=2) + "\n")
    return path
