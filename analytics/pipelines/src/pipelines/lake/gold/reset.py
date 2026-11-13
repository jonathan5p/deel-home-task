import os
from collections.abc import Iterable, Mapping

from pyspark.sql import SparkSession

from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.job import iceberg_gold
from pipelines.lake.gold.lake import IcebergGold
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA
from pipelines.lake.gold.stored import StoredTable
from pipelines.lake.gold.views.views import GoldLakeViews, drop_gold_views
from pipelines.settings import GoldSettings

LEGACY_PREFIX = "internal_"


def legacy_tables(settings: GoldSettings, names: Iterable[str]) -> dict[str, str]:
    """The tables the gold job wrote in the gold namespace, named with a prefix, before its
    tables moved to gold_internal; a reset drops them if they are still there."""
    return {name: settings.table(f"{LEGACY_PREFIX}{name}") for name in names}


def reset(
    spark: SparkSession, settings: GoldSettings, tables: Mapping[str, StoredTable] = STORED
) -> None:
    """Drop every gold table in the lake with its files, the legacy ones too, and the gold
    views; the gold job creates the tables and views again."""
    iceberg_gold(spark, settings, tables).drop()
    IcebergGold(spark, tables, legacy_tables(settings, tables)).drop()
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
