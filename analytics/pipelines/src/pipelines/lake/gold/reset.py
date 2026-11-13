import os
from collections.abc import Mapping

from pyspark.sql import SparkSession

from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.job import iceberg_gold
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA
from pipelines.lake.gold.stored import StoredTable
from pipelines.lake.gold.views.views import GoldLakeViews, drop_gold_views
from pipelines.settings import GoldSettings


def reset(
    spark: SparkSession, settings: GoldSettings, tables: Mapping[str, StoredTable] = STORED
) -> None:
    """Drop every gold table in the lake with its files, and the gold views; the gold job
    creates the tables and views again."""
    iceberg_gold(spark, settings, tables).drop()
    drop_gold_views(spark, GoldLakeViews(settings.lake))


def main() -> None:
    settings = GoldSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("gold-reset").getOrCreate()
    try:
        reset(spark, settings)
    finally:
        spark.stop()
    catalog = settings.lake.catalog
    print(
        f"Dropped the gold tables in {catalog}.{INTERNAL_SCHEMA}"
        f" and the gold views in {catalog}.{settings.LAYER}"
    )


if __name__ == "__main__":
    main()
