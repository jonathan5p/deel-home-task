from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.pipelines.tests.lake.gold.quality_setup import GoldTables


@pytest.fixture(scope="module")
def gold_tables(spark: SparkSession) -> GoldTables:
    """Every stored gold table in a consistent state, as the gold job writes them."""
    from analytics.pipelines.tests.lake.gold.quality_setup import consistent_gold

    return consistent_gold(spark)
