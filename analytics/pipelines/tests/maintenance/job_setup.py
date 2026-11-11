import os
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import Row, SparkSession

from analytics.pipelines.tests.gold.lake.views_setup import GoldLake
from analytics.pipelines.tests.lake.views_setup import register_view_catalog
from analytics.pipelines.tests.silver.views_setup import append_versions, version
from pipelines.bronze.entities import CUSTOMERS
from pipelines.lake.columns import ChangeOp
from pipelines.lake.views import ensure_view
from pipelines.maintenance.job import Clock
from pipelines.settings import MaintenanceSettings, SilverSettings
from pipelines.silver.views import refresh_views, silver_views

ROWS_SCHEMA = "id INT, state STRING"


def maintenance_settings(catalog: str, **overrides: str) -> MaintenanceSettings:
    return MaintenanceSettings.from_env(
        {
            "ICEBERG_CATALOG": catalog,
            "LAKE_ROOT": "file:///lake",
            "MAINTENANCE_DOWNSTREAM_CHECKPOINTS": "elsewhere.table=file:///nowhere",
        }
        | overrides
    )


def downstream(table: str, checkpoint: Path) -> dict[str, str]:
    return {"MAINTENANCE_DOWNSTREAM_CHECKPOINTS": f"{table}=file://{checkpoint}"}


def hours_from_now(hours: float) -> Clock:
    moment = datetime.now(UTC) + timedelta(hours=hours)
    return lambda: moment


def small_appends(spark: SparkSession, table: str, count: int, first_id: int = 0) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for row_id in range(first_id, first_id + count):
        rows = spark.createDataFrame([(row_id, "a")], ROWS_SCHEMA)
        if spark.catalog.tableExists(table):
            rows.writeTo(table).append()
        else:
            rows.writeTo(table).using("iceberg").create()


def data_files(spark: SparkSession, table: str) -> int:
    return int(spark.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0])


def snapshot_operations(spark: SparkSession, table: str) -> list[str]:
    return [
        row.operation
        for row in spark.sql(
            f"SELECT operation FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def table_rows(spark: SparkSession, table: str) -> list[Row]:
    return spark.table(table).orderBy("id").collect()


def leave_file(folder: Path, name: str, age_hours: float) -> Path:
    """A data file no snapshot references, with its modification time `age_hours` ago."""
    path = folder / name
    path.write_bytes(b"not referenced by any snapshot")
    modified = time.time() - age_hours * 3600
    os.utime(path, (modified, modified))
    return path


def stream_into(spark: SparkSession, table: str, output: Path) -> list[Row]:
    """Everything a stream reading the table from its checkpoint has delivered so far."""
    query = (
        spark.readStream.format("iceberg")
        .load(table)
        .writeStream.format("parquet")
        .option("path", str(output / "rows"))
        .option("checkpointLocation", str(output / "checkpoint"))
        .trigger(processingTime="1 second")
        .start()
    )
    query.processAllAvailable()
    query.stop()
    return spark.read.parquet(str(output / "rows")).orderBy("id").collect()


class FakeSql:
    def __init__(
        self,
        tables: Mapping[str, list[str]],
        failing: frozenset[str] = frozenset(),
        expiring: Mapping[int, bool] | None = None,
    ) -> None:
        self.tables = tables
        self.failing = failing
        self.expiring = expiring or {}
        self.statements: list[str] = []

    def __call__(self, statement: str) -> list[Row]:
        self.statements.append(statement)
        if statement.startswith("SHOW NAMESPACES"):
            return [Row(namespace=namespace) for namespace in self.tables]
        if statement.startswith("SHOW TABLES"):
            namespace = statement.rsplit(".", 1)[1]
            return [Row(namespace=namespace, tableName=name) for name in self.tables[namespace]]
        if ".snapshots)" in statement:
            return [
                Row(snapshot_id=id_, expiring=expiring)
                for id_, expiring in self.expiring.items()
                if str(id_) in statement.rsplit("IN", 1)[1]
            ]
        if any(f"table => '{table}'" in statement for table in self.failing):
            raise RuntimeError("table metadata is unreadable")
        if ".rewrite_data_files(" in statement:
            return [Row(rewritten_data_files_count=6, added_data_files_count=1)]
        if ".expire_snapshots(" in statement:
            return [Row(deleted_manifest_lists_count=4)]
        return [Row(orphan_file_location="s3://lake/warehouse/stray.parquet")]


def stale_silver_views(spark: SparkSession, catalog: str, lake_root: str) -> SilverSettings:
    """Customers' silver table with its views, the history view carrying a stale description."""
    settings = SilverSettings.from_env(
        {
            "KAFKA_TOPICS": "test.operations.customers",
            "LAKE_ROOT": lake_root,
            "ICEBERG_CATALOG": catalog,
        }
    )
    append_versions(spark, settings, [version(1, ChangeOp.CREATE)])
    refresh_views(spark, settings)
    history, definition = next(iter(silver_views(CUSTOMERS, settings).items()))
    ensure_view(spark, history, definition._replace(comment="A stale description"))
    return settings


def stale_gold_view(spark: SparkSession, folder: Path) -> tuple[GoldLake, str]:
    """A lake with its silver and gold views, one gold view carrying a stale description."""
    gold = GoldLake.create(spark, folder, register_view_catalog(spark, folder))
    gold.refresh()
    view = gold.view("open_orders")
    ensure_view(spark, view, gold.views.definitions()[view]._replace(comment="A stale description"))
    return gold, view
