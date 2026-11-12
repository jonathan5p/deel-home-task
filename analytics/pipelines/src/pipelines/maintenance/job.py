import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from pyspark.sql import Row, SparkSession

from pipelines.lake.gold.views.views import GoldLakeViews, refresh_gold_views
from pipelines.lake.shared.checkpoints import (
    CheckpointFiles,
    HadoopCheckpointFiles,
    committed_snapshots,
)
from pipelines.lake.silver.views import MissingSilverTablesError, refresh_views
from pipelines.settings import MaintenanceSettings, SilverSettings

type Sql = Callable[[str], list[Row]]
type Clock = Callable[[], datetime]
type Report = Callable[[str], None]
type ViewRefresh = Callable[[], Sequence[str]]

SILVER_VIEWS = "silver views"
GOLD_VIEWS = "gold views"


class TableMaintenance(NamedTuple):
    table: str
    rewritten_files: int
    added_files: int
    expired_snapshots: int
    removed_orphans: int
    expiry_skipped: bool = False

    def summary(self) -> str:
        expiry = (
            "kept every snapshot for a lagging stream"
            if self.expiry_skipped
            else f"expired {self.expired_snapshots} snapshots"
        )
        return (
            f"{self.table}: rewrote {self.rewritten_files} data files into {self.added_files},"
            f" {expiry}, removed {self.removed_orphans} orphan files"
        )


class MaintenanceFailedError(RuntimeError):
    def __init__(self, tables: Sequence[str]) -> None:
        self.tables = tuple(tables)
        super().__init__(f"maintenance failed for: {', '.join(self.tables)}")


class Maintenance(NamedTuple):
    sql: Sql
    settings: MaintenanceSettings
    checkpoints: CheckpointFiles
    report: Report


def spark_sql(spark: SparkSession) -> Sql:
    return lambda statement: spark.sql(statement).collect()


def utc_now() -> datetime:
    return datetime.now(UTC)


def sql_timestamp(moment: datetime) -> str:
    return f"TIMESTAMP '{moment.astimezone(UTC).isoformat(sep=' ')}'"


def tables(sql: Sql, settings: MaintenanceSettings) -> list[str]:
    """The tables of every configured namespace; a namespace that does not exist yet is skipped."""
    existing = {row[0] for row in sql(f"SHOW NAMESPACES IN {settings.catalog}")}
    return sorted(
        f"{namespace}.{row.tableName}"
        for namespace in settings.namespaces
        if namespace in existing
        for row in sql(f"SHOW TABLES IN {settings.catalog}.{namespace}")
    )


def expiry_blockers(job: Maintenance, table: str, cutoff: str) -> list[str]:
    """Why expiring snapshots older than `cutoff` would break a stream reading `table`."""
    blockers: list[str] = []
    for checkpoint in job.settings.checkpoints(table):
        snapshot_ids = committed_snapshots(job.checkpoints, checkpoint)
        if snapshot_ids is None:
            job.report(f"{table}: no committed batch in {checkpoint} yet, so no constraint")
            continue
        expiring = {
            row.snapshot_id: row.expiring
            for row in job.sql(
                "SELECT snapshot_id, committed_at < "
                f"{cutoff} AND newest > {job.settings.retain_last} AS expiring FROM ("
                "SELECT snapshot_id, committed_at,"
                " row_number() OVER (ORDER BY committed_at DESC) AS newest"
                f" FROM {job.settings.catalog}.{table}.snapshots)"
                f" WHERE snapshot_id IN ({', '.join(str(id_) for id_ in snapshot_ids)})"
            )
        }
        for snapshot_id in snapshot_ids:
            if snapshot_id not in expiring:
                blockers.append(
                    f"{checkpoint} needs snapshot {snapshot_id}, which no longer exists"
                )
            elif expiring[snapshot_id]:
                blockers.append(f"{checkpoint} still needs snapshot {snapshot_id}")
    return blockers


def maintain(job: Maintenance, table: str, now: datetime) -> TableMaintenance:
    settings = job.settings
    procedures = f"{settings.catalog}.system"
    snapshots_before = sql_timestamp(now - timedelta(hours=settings.snapshot_retention_hours))
    orphans_before = sql_timestamp(now - timedelta(hours=settings.orphan_age_hours))
    (rewritten,) = job.sql(
        f"CALL {procedures}.rewrite_data_files(table => '{table}', strategy => 'binpack')"
    )
    blockers = expiry_blockers(job, table, snapshots_before)
    for blocker in blockers:
        job.report(f"{table}: WARNING skipping snapshot expiry: {blocker}")
    expired_snapshots = 0
    if not blockers:
        (expired,) = job.sql(
            f"CALL {procedures}.expire_snapshots(table => '{table}',"
            f" older_than => {snapshots_before}, retain_last => {settings.retain_last})"
        )
        expired_snapshots = expired["deleted_manifest_lists_count"]
    orphans = job.sql(
        f"CALL {procedures}.remove_orphan_files(table => '{table}',"
        f" older_than => {orphans_before}, prefix_listing => true)"
    )
    return TableMaintenance(
        table=table,
        rewritten_files=rewritten["rewritten_data_files_count"],
        added_files=rewritten["added_data_files_count"],
        expired_snapshots=expired_snapshots,
        removed_orphans=len(orphans),
        expiry_skipped=bool(blockers),
    )


def refresh_lake_views(views: Mapping[str, ViewRefresh] | None, report: Report) -> list[str]:
    """Refresh each named group of views in order; returns the groups that failed.

    Every group reads the silver tables, so once one reports them missing the rest are skipped.
    """
    failed: list[str] = []
    missing: MissingSilverTablesError | None = None
    for name, refresh in (views or {}).items():
        if missing is not None:
            report(f"{name}: skipped: {missing}")
            continue
        try:
            statements = refresh()
        except MissingSilverTablesError as error:
            missing = error
            report(f"{name}: skipped: {error}")
        except Exception as error:
            report(f"{name}: refresh failed: {error}")
            failed.append(name)
        else:
            report(
                f"{name}: created or replaced {len(statements)}"
                if statements
                else f"{name}: up to date"
            )
    return failed


def run(
    sql: Sql,
    settings: MaintenanceSettings,
    checkpoints: CheckpointFiles,
    *,
    now: Clock = utc_now,
    report: Report = print,
    views: Mapping[str, ViewRefresh] | None = None,
) -> list[TableMaintenance]:
    job = Maintenance(sql=sql, settings=settings, checkpoints=checkpoints, report=report)
    moment = now()
    maintained: list[TableMaintenance] = []
    failed: list[str] = []
    for table in tables(sql, settings):
        try:
            result = maintain(job, table, moment)
        except Exception as error:
            report(f"{table}: maintenance failed: {error}")
            failed.append(table)
        else:
            report(result.summary())
            maintained.append(result)
    failed += refresh_lake_views(views, report)
    if failed:
        raise MaintenanceFailedError(failed)
    return maintained


def main() -> None:
    settings = MaintenanceSettings.from_env(os.environ)
    spark = SparkSession.builder.appName("lake-maintenance").getOrCreate()
    try:
        silver = SilverSettings.from_env(os.environ)
        run(
            spark_sql(spark),
            settings,
            HadoopCheckpointFiles(spark),
            views={
                SILVER_VIEWS: lambda: refresh_views(spark, silver),
                GOLD_VIEWS: lambda: refresh_gold_views(spark, GoldLakeViews(silver.lake)),
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
