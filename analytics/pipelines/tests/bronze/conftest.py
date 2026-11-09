from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pipelines.settings import BronzeSettings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQueryManager

    from analytics.pipelines.tests.bronze.job_setup import BronzeSettingsFactory, FakeCatalog


@pytest.fixture
def bronze_settings(tmp_path: Path) -> BronzeSettingsFactory:
    def settings(*topics: str, catalog: str = "catalog") -> BronzeSettings:
        return BronzeSettings.from_env(
            {
                "KAFKA_TOPICS": ",".join(topics),
                "LAKE_ROOT": f"file://{tmp_path}/lake",
                "ICEBERG_CATALOG": catalog,
            }
        )

    return settings


@pytest.fixture
def fake_catalog(tmp_path: Path) -> FakeCatalog:
    from analytics.pipelines.tests.bronze.job_setup import FakeCatalog

    return FakeCatalog(tmp_path / "tables")


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
