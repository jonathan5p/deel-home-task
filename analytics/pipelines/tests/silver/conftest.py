from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pipelines.settings import SilverSettings
from pipelines.silver.job import JOB_SESSION_CONF

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.pipelines.tests.silver.job_setup import SilverSettingsFactory
    from pipelines.bronze.entities import Entity


@pytest.fixture
def silver_settings(tmp_path: Path) -> SilverSettingsFactory:
    from analytics.pipelines.tests.silver.job_setup import topic

    def settings(*entities: Entity, catalog: str = "catalog") -> SilverSettings:
        return SilverSettings.from_env(
            {
                "KAFKA_TOPICS": ",".join(topic(entity) for entity in entities),
                "LAKE_ROOT": f"file://{tmp_path}/lake",
                "ICEBERG_CATALOG": catalog,
            }
        )

    return settings


@pytest.fixture
def job_session_conf(spark: SparkSession) -> Iterator[None]:
    """The job sets its state settings on the session it runs in, which every test shares; put
    back the previous values afterwards."""
    previous = {name: spark.conf.get(name, None) for name in JOB_SESSION_CONF}
    yield
    for name, value in previous.items():
        if value is None:
            spark.conf.unset(name)
        else:
            spark.conf.set(name, value)
