import os
import sys
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from pipelines.quality.checks import CheckResult, all_passed, report_lines
from pipelines.quality.landing import read_kafka, read_landing, reconcile
from pipelines.settings import KafkaSettings, LandingQualitySettings, LandingSettings

REPORT_PREFIX = "LANDING QUALITY"

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
    kafka = read_records(spark, settings.landing.kafka)
    return reconcile(landed, kafka, last_offsets=settings.last_offsets)


def main() -> int:
    settings = LandingQualitySettings.from_env(os.environ)
    spark = SparkSession.builder.appName("landing-quality").getOrCreate()
    results = run_checks(spark, settings)
    spark.stop()
    for line in report_lines(REPORT_PREFIX, results):
        print(line, flush=True)
    return 0 if all_passed(results) else 1


if __name__ == "__main__":
    sys.exit(main())
