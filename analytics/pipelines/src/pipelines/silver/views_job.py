import os

from pyspark.sql import SparkSession

from pipelines.gold.lake.views import GoldLakeViews, refresh_gold_views
from pipelines.settings import SilverSettings
from pipelines.silver.views import refresh_views


def main() -> None:
    settings = SilverSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("lake-views").getOrCreate()
    try:
        silver = refresh_views(spark, settings)
        gold = refresh_gold_views(spark, GoldLakeViews(settings.lake))
    finally:
        spark.stop()
    print(f"SILVER VIEWS {len(silver)} created or replaced")
    print(f"GOLD VIEWS {len(gold)} created or replaced")


if __name__ == "__main__":
    main()
