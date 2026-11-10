from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQueryManager


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    sql = pytest.importorskip("pyspark.sql")
    session = (
        sql.SparkSession.builder.master("local[2]")
        .appName("pipelines-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def schemas(tmp_path: Path) -> SchemasTree:
    return SchemasTree(tmp_path / "schemas")


@pytest.fixture
def iceberg_catalog(spark: SparkSession, tmp_path: Path) -> str:
    """Register an Iceberg catalog on local files; the image ships the Iceberg runtime."""
    name = f"iceberg_{tmp_path.name.replace('-', '_')}"
    spark.conf.set(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{name}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{name}.warehouse", f"file://{tmp_path}/warehouse")
    return name


@pytest.fixture
def streams(spark: SparkSession) -> StreamingQueryManager:
    """The session's query manager, forgetting queries that earlier tests terminated."""
    spark.streams.resetTerminated()
    return spark.streams
