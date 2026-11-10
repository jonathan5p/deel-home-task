import os

from pyspark.sql import SparkSession

from pipelines.settings import SilverSettings
from pipelines.silver.views import refresh_views


def main() -> None:
    settings = SilverSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("silver-views").getOrCreate()
    try:
        statements = refresh_views(spark, settings)
    finally:
        spark.stop()
    print(f"SILVER VIEWS {len(statements)} created or replaced")


if __name__ == "__main__":
    main()
