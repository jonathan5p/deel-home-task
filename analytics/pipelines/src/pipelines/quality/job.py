import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence

from pyspark.sql import SparkSession

from pipelines.bronze import quality as bronze
from pipelines.gold import quality as gold
from pipelines.landing import quality as landing
from pipelines.quality.checks import Check, CheckResult, all_passed, report_lines, summary_object
from pipelines.quality.report import Results, write_report
from pipelines.settings import (
    BronzeSettings,
    GoldSettings,
    LakeSettings,
    LandingQualitySettings,
    SilverSettings,
)
from pipelines.silver import quality as silver

ALL = "all"
SUMMARY_PREFIX = "DATA QUALITY"
ERROR_MESSAGE_LIMIT = 1000

CHECK_COMPLETED = Check("check_completed", "The check ran to the end without raising an error.")

type Env = Mapping[str, str]
type QualityCheck = Callable[[SparkSession, Env], Sequence[CheckResult]]
type ReportWriter = Callable[[SparkSession, LakeSettings, Results], str]


def landing_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return landing.run_checks(spark, LandingQualitySettings.from_env(env))


def bronze_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return bronze.run_checks(spark, BronzeSettings.from_env(env))


def silver_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return silver.run_checks(spark, SilverSettings.from_env(env))


def gold_checks(spark: SparkSession, env: Env) -> list[CheckResult]:
    return gold.run_checks(spark, GoldSettings.from_env(env))


CHECKS: Mapping[str, QualityCheck] = {
    "landing": landing_checks,
    "bronze": bronze_checks,
    "silver": silver_checks,
    "gold": gold_checks,
}


def selected_checks(argv: Sequence[str], checks: Mapping[str, QualityCheck]) -> list[str]:
    """The checks named on the command line, in registry order; `all` or none means every one."""
    parser = argparse.ArgumentParser(prog="data-quality")
    parser.add_argument("checks", nargs="*", choices=[*checks, ALL], metavar="check")
    names = set(parser.parse_args(argv).checks)
    return [name for name in checks if not names or ALL in names or name in names]


def guarded(name: str, check: QualityCheck, spark: SparkSession, env: Env) -> Sequence[CheckResult]:
    """The check's results, or one failed result naming the error it raised, so a broken check
    still leaves a report."""
    try:
        return check(spark, env)
    except Exception as error:
        details = {"error": type(error).__name__, "message": str(error)[:ERROR_MESSAGE_LIMIT]}
        return [CHECK_COMPLETED.result(name, passed=False, metrics={}, details=details)]


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
    results = {name: guarded(name, checks[name], spark, env) for name in names}
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
