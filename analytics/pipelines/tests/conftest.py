from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SPARK_TESTS = {"test_spark_runtime.py"}


def pytest_ignore_collect(collection_path: Path) -> bool | None:
    """Skip Spark tests where PySpark is unavailable, such as a plain host virtualenv."""
    if collection_path.name not in SPARK_TESTS:
        return None
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return True
    return None


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
