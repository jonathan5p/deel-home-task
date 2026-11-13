import pytest

pytest.importorskip("pyspark")

from pathlib import Path

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.rebuild_setup import gold_in
from analytics.pipelines.tests.lake.shared.views_setup import stored_views
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.job import iceberg_gold
from pipelines.lake.gold.lake import IcebergGold
from pipelines.lake.gold.reset import legacy_tables, reset
from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views
from pipelines.settings import GoldSettings

SETTINGS = GoldSettings.from_env(
    {
        "LAKE_ROOT": "file:///lake",
        "ICEBERG_CATALOG": "lake",
        "ANALYTICS_DB_URL": "jdbc:postgresql://db/gold",
        "ANALYTICS_DB_USER": "gold",
        "ANALYTICS_DB_PASSWORD": "secret",
    }
)


def test_the_legacy_tables_are_the_prefixed_ones_in_the_gold_namespace() -> None:
    assert legacy_tables(SETTINGS, ["dim_customer"]) == {
        "dim_customer": "lake.gold.internal_dim_customer"
    }


def test_a_reset_drops_the_gold_tables_the_legacy_ones_and_the_views(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = gold_in(spark, tmp_path)
    iceberg_gold(spark, settings).ensure()
    IcebergGold(spark, STORED, legacy_tables(settings, STORED)).ensure()
    views = GoldLakeViews(settings.lake)
    refresh_gold_views(spark, views)

    reset(spark, settings)
    reset(spark, settings)

    namespace = settings.lake.catalog
    assert spark.sql(f"SHOW TABLES IN {namespace}.gold_internal").collect() == []
    assert spark.sql(f"SHOW TABLES IN {namespace}.gold").collect() == []
    assert stored_views(spark, f"{namespace}.gold") == []
