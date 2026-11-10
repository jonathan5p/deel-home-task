from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.pipelines.tests.gold.lake.views_setup import GoldLake


def gold_lake_in(spark: SparkSession, folder: Path) -> GoldLake:
    from analytics.pipelines.tests.gold.lake.views_setup import GoldLake
    from analytics.pipelines.tests.lake.views_setup import register_view_catalog

    lake = GoldLake.create(spark, folder, register_view_catalog(spark, folder))
    lake.refresh()
    return lake


@pytest.fixture(scope="module")
def gold_lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> GoldLake:
    return gold_lake_in(spark, tmp_path_factory.mktemp("gold_lake"))


@pytest.fixture
def empty_gold_lake(spark: SparkSession, tmp_path: Path) -> GoldLake:
    return gold_lake_in(spark, tmp_path)
