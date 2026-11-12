from pathlib import Path

from pyspark.sql import SparkSession

from pipelines.lake.shared.views import ViewDefinition
from pipelines.schemas.definitions import ColumnDefinition, ColumnType


def numbers_table(spark: SparkSession, catalog: str) -> str:
    """A table of the numbers 1 to 3, for views to read."""
    table = f"{catalog}.example.numbers"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.example")
    spark.createDataFrame([(1,), (2,), (3,)], "number BIGINT").writeTo(table).create()
    return table


def even_numbers(table: str) -> ViewDefinition:
    return ViewDefinition(
        comment="The even numbers, it's \\ said",
        columns={
            "number": ColumnDefinition("number", ColumnType.LONG, "An even number"),
            "half": ColumnDefinition("half", ColumnType.LONG, "Half of it, the number's half"),
        },
        query=f"SELECT number, number DIV 2\nFROM {table}\nWHERE number % 2 = 0",
    )


def view_metadata_file(spark: SparkSession, catalog: str, view: str) -> str:
    """The view's current metadata file in the Iceberg catalog; every commit writes a new one.

    Spark SQL shows no view metadata, so this asks the catalog through the JVM.
    """
    catalogs = spark._jsparkSession.sessionState().catalogManager()
    iceberg = catalogs.catalog(catalog).icebergCatalog()
    jvm = spark.sparkContext._jvm
    assert jvm is not None
    name = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(view.removeprefix(f"{catalog}."))
    return str(iceberg.loadView(name).operations().current().metadataFileLocation())


def register_view_catalog(spark: SparkSession, folder: Path) -> str:
    """Register an in-memory Iceberg catalog, which unlike the Hadoop catalog stores views.

    Without the caching wrapper, tests can reach the catalog's own view API.
    """
    name = f"iceberg_views_{folder.name.replace('-', '_')}"
    prefix = f"spark.sql.catalog.{name}"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{prefix}.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
    spark.conf.set(f"{prefix}.cache-enabled", "false")
    spark.conf.set(f"{prefix}.warehouse", f"file://{folder}/warehouse")
    return name
