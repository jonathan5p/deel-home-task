import sys

import pyspark
from pyspark.sql import SparkSession

EXPECTED_SPARK = "4.1.4"
EXPECTED_PYTHON = (3, 14)


def test_pyspark_matches_the_spark_runtime(spark: SparkSession) -> None:
    assert pyspark.__version__ == EXPECTED_SPARK
    assert spark.version == EXPECTED_SPARK


def test_driver_runs_the_expected_python() -> None:
    assert sys.version_info[:2] == EXPECTED_PYTHON


def test_python_workers_run_the_same_python_as_the_driver(spark: SparkSession) -> None:
    # Defined inside the test so Spark ships it by value; workers cannot import test modules.
    def python_version(_: int) -> tuple[int, int]:
        return sys.version_info.major, sys.version_info.minor

    versions = set(spark.sparkContext.parallelize(range(4), 2).map(python_version).collect())

    assert versions == {EXPECTED_PYTHON}
