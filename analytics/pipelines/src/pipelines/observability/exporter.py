import logging
import os
import time
from collections.abc import Mapping

from prometheus_client import CollectorRegistry, disable_created_metrics, start_http_server
from pyiceberg.catalog import load_catalog

from pipelines.observability.schedule import Task, run_tasks
from pipelines.observability.stage_exporter import StageExporter, StageMetrics, http_fetch
from pipelines.observability.table_stats import IcebergTables, TableMetrics, TableStatsCollector
from pipelines.settings import StageExporterSettings, TableStatsSettings


def tasks(
    stages: StageExporterSettings, tables: TableStatsSettings, registry: CollectorRegistry
) -> list[Task]:
    """The exporter's periodic work: the completed stages, then the Iceberg tables as a whole
    and per partition, each on its own interval."""
    exporter = StageExporter(
        http_fetch(stages.history_server_url), StageMetrics(registry), time.time()
    )
    catalog = load_catalog(tables.catalog_name, **tables.catalog_properties())
    collector = TableStatsCollector(
        IcebergTables(catalog), TableMetrics(registry), tables.namespaces, tables.small_file_bytes
    )
    return [
        Task("completed stages recorded", stages.poll_seconds, exporter.poll),
        Task("tables measured", tables.table_interval_seconds, collector.collect_tables),
        Task(
            "tables measured per partition",
            tables.partition_interval_seconds,
            collector.collect_partitions,
        ),
    ]


def main(env: Mapping[str, str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    disable_created_metrics()
    stages = StageExporterSettings.from_env(env)
    registry = CollectorRegistry()
    work = tasks(stages, TableStatsSettings.from_env(env), registry)
    start_http_server(stages.port, registry=registry)
    run_tasks(work)


if __name__ == "__main__":
    main(os.environ)
