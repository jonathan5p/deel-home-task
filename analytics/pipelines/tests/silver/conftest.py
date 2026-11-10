from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pipelines.settings import SilverSettings

if TYPE_CHECKING:
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
