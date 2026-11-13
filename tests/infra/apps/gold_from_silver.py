import json
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from common.settings import AnalyticsDbSettings
from pipelines.lake.gold import job, rebuild
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.lake.gold.public import FRESHNESS
from pipelines.lake.gold.sink import jdbc_options
from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views
from pipelines.lake.shared.tables import table_definition
from pipelines.lake.shared.types import SPARK_TYPES
from pipelines.schemas.definitions import EntityDefinition
from pipelines.settings import GoldSettings, SilverSettings

LAKE_CATALOG = "spark.sql.catalog.lake"
LAKE_VIEWS = "--lake-views"
STREAM = "--stream"
COMPARED = "GOLD LAKE "
SEEDED = ("dim_date", "dim_order_status")


def silver_schema(entity: EntityDefinition) -> StructType:
    """The real silver table's columns, from the silver template."""
    columns = table_definition("silver", entity).columns.items()
    return StructType([StructField(name, SPARK_TYPES[column.type]) for name, column in columns])


def register_fixture_catalog(spark: SparkSession, catalog: str, warehouse: str) -> None:
    """Register a copy of the lake catalog under another name, with its own warehouse, so the
    fixture silver tables never meet the real ones. Spark resolves a catalog's settings on its
    first use, so registering it on a running session works; it caches no tables, as the gold
    job's catalog does not."""
    for key, value in spark.sparkContext.getConf().getAll():
        if key.startswith(f"{LAKE_CATALOG}."):
            spark.conf.set(f"spark.sql.catalog.{catalog}{key.removeprefix(LAKE_CATALOG)}", value)
    spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    spark.conf.set(f"spark.sql.catalog.{catalog}.cache-enabled", "false")


def append_versions(spark: SparkSession, catalog: str, folder: str) -> None:
    """Append `<folder>/<entity>.json` (JSON lines) to the entity's fixture silver table."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    for entity in (load.entity for load in (*DIMENSIONS, *FACTS)):
        schema = silver_schema(entity)
        table = f"{catalog}.silver.{entity.name}"
        if not spark.catalog.tableExists(table):
            spark.createDataFrame([], schema).writeTo(table).create()
        spark.read.schema(schema).json(f"{folder}/{entity.name}.json").writeTo(table).append()


def matching(lake: DataFrame, postgres: DataFrame, *, every_row: bool) -> dict[str, int]:
    """The rows of a lake view, and the rows it or its PostgreSQL view has that the other lacks."""
    postgres = postgres.select(*(F.col(f.name).cast(f.dataType) for f in lake.schema.fields))
    if not every_row:
        keys = lake.select(F.col(lake.columns[0]).alias("lake_key")).distinct()
        matched = postgres[lake.columns[0]].eqNullSafe(keys["lake_key"])
        postgres = postgres.join(keys, matched, "left_semi")
    return {
        "rows": lake.count(),
        "lake_only": lake.exceptAll(postgres).count(),
        "postgres_only": postgres.exceptAll(lake).count(),
    }


def compare_lake_views(spark: SparkSession) -> None:
    """Print how many gold views the gold job left and how many a refresh replaces, the
    entities with a freshness time, then each view's comparison with PostgreSQL's."""
    settings = SilverSettings.from_env(os.environ)
    views = GoldLakeViews(settings.lake)
    stored = spark.sql(f"SHOW VIEWS IN {views.namespace}").count()
    replaced = refresh_gold_views(spark, views)
    print(f"{COMPARED}{json.dumps({'views': stored, 'replaced': len(replaced)})}")
    freshness = spark.table(views.view(FRESHNESS)).where("last_changed_at IS NOT NULL")
    fresh = sorted(row.entity for row in freshness.collect())
    print(f"{COMPARED}{json.dumps({'fresh': fresh})}")
    database = AnalyticsDbSettings.from_env(os.environ)
    for view in views.definitions():
        name = view.rpartition(".")[2]
        if name == FRESHNESS:
            continue
        options = jdbc_options(database, f"gold.{name}")
        postgres = spark.read.format("jdbc").options(**options).load()
        compared = matching(spark.table(view), postgres, every_row=name in SEEDED)
        print(f"{COMPARED}{json.dumps({'view': name} | compared)}", flush=True)


def stream_batches(
    spark: SparkSession, catalog: str, key: str, pause: float, batches: list[str]
) -> None:
    """Start the gold job's streams after appending the first batch, then append each further
    batch after `pause` seconds, so every batch after the first runs in a warm driver as the
    long-running gold job does; stop the streams after the last one."""
    gold = GoldSettings.from_env(os.environ)
    first, *rest = batches
    append_versions(spark, catalog, f"s3a://lake/{key}/silver/{first}")
    writer, lake = job.gold_writer(spark, gold)
    job.prepare(spark, writer, lake)
    queries = job.run(spark, gold, job.writer_sinks(writer))
    time.sleep(pause)
    for name in rest:
        append_versions(spark, catalog, f"s3a://lake/{key}/silver/{name}")
        time.sleep(pause)
    for query in queries:
        query.stop()


def main(argv: list[str]) -> None:
    """Append batches of silver versions to the fixture silver tables, if any are named, then
    run the gold job on them with --available-now; or, after --rebuild, run the rebuild job on
    them with the arguments that follow it; or, with --lake-views, compare the gold views in the
    lake with PostgreSQL's; or, with --stream and a pause in seconds, stream the batches into
    the gold job one after the other (`stream_batches`).

    Arguments: the fixture folder's key in the lake bucket, then the batches' folder names or
    --rebuild and the rebuild job's arguments, or --lake-views, or --stream, the pause and the
    batches' folder names.
    """
    key, *batch = argv
    catalog = os.environ["ICEBERG_CATALOG"]
    builder = SparkSession.builder.appName("gold-from-silver")
    conf = job.session_conf(GoldSettings.from_env(os.environ))
    for name, value in {**conf, "spark.sql.session.timeZone": "UTC"}.items():
        builder = builder.config(name, value)
    spark = builder.getOrCreate()
    register_fixture_catalog(spark, catalog, f"s3://lake/{key}/warehouse")
    if batch[:1] == [STREAM]:
        job.log_batches()
        try:
            stream_batches(spark, catalog, key, float(batch[1]), batch[2:])
        finally:
            spark.stop()
        return
    if batch == [LAKE_VIEWS]:
        compare_lake_views(spark)
        spark.stop()
        return
    if batch[:1] == ["--rebuild"]:
        rebuild.main(batch[1:])
        return
    for name in batch:
        append_versions(spark, catalog, f"s3a://lake/{key}/silver/{name}")
    job.main(["--available-now"])


if __name__ == "__main__":
    main(sys.argv[1:])
