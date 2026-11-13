import pytest

pytest.importorskip("pyspark")

from pathlib import Path

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.rebuild_setup import gold_in
from analytics.pipelines.tests.lake.shared.views_setup import stored_views
from pipelines.lake.gold.job import iceberg_gold
from pipelines.lake.gold.reset import reset
from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views


def test_a_reset_drops_the_gold_tables_and_the_views(spark: SparkSession, tmp_path: Path) -> None:
    settings = gold_in(spark, tmp_path)
    iceberg_gold(spark, settings).ensure()
    views = GoldLakeViews(settings.lake)
    refresh_gold_views(spark, views)

    reset(spark, settings)
    reset(spark, settings)

    namespace = settings.lake.catalog
    assert spark.sql(f"SHOW TABLES IN {namespace}.gold_internal").collect() == []
    assert spark.sql(f"SHOW TABLES IN {namespace}.gold").collect() == []
    assert stored_views(spark, f"{namespace}.gold") == []
