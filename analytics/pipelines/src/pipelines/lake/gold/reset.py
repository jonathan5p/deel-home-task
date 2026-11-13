import os

from pyspark.sql import SparkSession

from pipelines.lake.gold.job import iceberg_gold
from pipelines.settings import GoldSettings


def main() -> None:
    """Drop every gold table in the lake with its files."""
    settings = GoldSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("gold-reset").getOrCreate()
    try:
        iceberg_gold(spark, settings).drop()
    finally:
        spark.stop()
    print(f"Dropped the gold tables in {settings.lake.catalog}.{settings.LAYER}")


if __name__ == "__main__":
    main()
