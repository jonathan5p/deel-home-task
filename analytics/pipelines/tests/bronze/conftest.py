from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pipelines.settings import BronzeSettings

if TYPE_CHECKING:
    from analytics.pipelines.tests.bronze.job_setup import BronzeSettingsFactory
    from analytics.pipelines.tests.lake.streams_setup import FakeCatalog


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
    from analytics.pipelines.tests.bronze.job_setup import source_coordinates
    from analytics.pipelines.tests.lake.streams_setup import FakeCatalog

    return FakeCatalog(tmp_path / "tables", order_by=source_coordinates)
