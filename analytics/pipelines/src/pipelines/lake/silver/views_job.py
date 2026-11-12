import os

from pyspark.sql import SparkSession

from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views
from pipelines.lake.silver.views import refresh_views
from pipelines.settings import SilverSettings


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
