import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence

from pyspark.sql import SparkSession

from pipelines.bronze import quality as bronze
from pipelines.landing import quality as landing
from pipelines.quality.checks import CheckResult, all_passed, report_lines, summary_object
from pipelines.quality.report import Results, write_report
from pipelines.settings import BronzeSettings, LakeSettings, LandingQualitySettings

ALL = "all"
SUMMARY_PREFIX = "DATA QUALITY"

type Env = Mapping[str, str]
type QualityCheck = Callable[[SparkSession, Env], Sequence[CheckResult]]
type ReportWriter = Callable[[SparkSession, LakeSettings, Results], str]


def landing_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return landing.run_checks(spark, LandingQualitySettings.from_env(env))


def bronze_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return bronze.run_checks(spark, BronzeSettings.from_env(env))


CHECKS: Mapping[str, QualityCheck] = {"landing": landing_checks, "bronze": bronze_checks}


def selected_checks(argv: Sequence[str], checks: Mapping[str, QualityCheck]) -> list[str]:
    """The checks named on the command line, in registry order; `all` or none means every one."""
    parser = argparse.ArgumentParser(prog="data-quality")
    parser.add_argument("checks", nargs="*", choices=[*checks, ALL], metavar="check")
    names = set(parser.parse_args(argv).checks)
    return [name for name in checks if not names or ALL in names or name in names]


def run(
    spark: SparkSession,
    env: Env,
    names: Sequence[str],
    *,
    checks: Mapping[str, QualityCheck] = CHECKS,
    write: ReportWriter = write_report,
    out: Callable[[str], None] = print,
) -> bool:
    """Run the named checks, print each one's results, write the run's report; True if all
    passed."""
    results = {name: checks[name](spark, env) for name in names}
    for name, check_results in results.items():
        for line in report_lines(f"{name.upper()} QUALITY", check_results):
            out(line)
    every = [result for check_results in results.values() for result in check_results]
    path = write(spark, LakeSettings.from_env(env), results)
    out(f"{SUMMARY_PREFIX} {json.dumps(summary_object(every) | {'report': path})}")
    return all_passed(every)


def main(argv: Sequence[str]) -> int:
    names = selected_checks(argv, CHECKS)
    spark = SparkSession.builder.appName("data-quality").getOrCreate()
    try:
        passed = run(spark, os.environ, names)
    finally:
        spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
