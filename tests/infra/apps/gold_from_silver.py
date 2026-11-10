import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from pipelines.gold import job
from pipelines.gold.dimensions import DIMENSIONS
from pipelines.lake.tables import table_definition
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.definitions import EntityDefinition

LAKE_CATALOG = "spark.sql.catalog.lake"


def silver_schema(entity: EntityDefinition) -> StructType:
    """The real silver table's columns, from the silver template."""
    columns = table_definition("silver", entity).columns.items()
    return StructType([StructField(name, SPARK_TYPES[column.type]) for name, column in columns])


def register_fixture_catalog(spark: SparkSession, catalog: str, warehouse: str) -> None:
    """Register a copy of the lake catalog under another name, with its own warehouse, so the
    fixture silver tables never meet the real ones. Spark resolves a catalog's settings on its
    first use, so registering it on a running session works."""
    for key, value in spark.sparkContext.getConf().getAll():
        if key.startswith(f"{LAKE_CATALOG}."):
            spark.conf.set(f"spark.sql.catalog.{catalog}{key.removeprefix(LAKE_CATALOG)}", value)
    spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", warehouse)


def append_versions(spark: SparkSession, catalog: str, folder: str) -> None:
    """Append `<folder>/<entity>.json` (JSON lines) to the entity's fixture silver table."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    for dimension in DIMENSIONS:
        entity = dimension.entity
        schema = silver_schema(entity)
        table = f"{catalog}.silver.{entity.name}"
        if not spark.catalog.tableExists(table):
            spark.createDataFrame([], schema).writeTo(table).create()
        spark.read.schema(schema).json(f"{folder}/{entity.name}.json").writeTo(table).append()


def main(argv: list[str]) -> None:
    """Append a batch of silver versions to the fixture silver tables, if one is named, then
    run the gold job on them with --available-now.

    Arguments: the fixture folder's key in the lake bucket, then the batch's folder name.
    """
    key, *batch = argv
    catalog = os.environ["ICEBERG_CATALOG"]
    spark = (
        SparkSession.builder.appName("gold-from-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    register_fixture_catalog(spark, catalog, f"s3://lake/{key}/warehouse")
    for name in batch:
        append_versions(spark, catalog, f"s3a://lake/{key}/silver/{name}")
    job.main(["--available-now"])


if __name__ == "__main__":
    main(sys.argv[1:])
