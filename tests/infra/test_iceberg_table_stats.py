from collections.abc import Iterator
from dataclasses import replace

import pytest

from tests.infra.prometheus_setup import (
    query,
    remove_prometheus,
    remove_stage_exporter,
    start_prometheus,
    start_stage_exporter,
)
from tests.infra.stack import Stack, wait_until

SMOKE_APP = "tests/infra/apps/iceberg_smoke_check.py"
SMOKE_TABLE = {"namespace": "smoke", "table": "iceberg_check"}
FAST_TABLE_STATS = {
    "TABLE_STATS_NAMESPACES": "smoke",
    "TABLE_STATS_INTERVAL_SECONDS": "5",
    "PARTITION_STATS_INTERVAL_SECONDS": "5",
}


def selector(metric: str) -> str:
    labels = ", ".join(f'{name}="{value}"' for name, value in SMOKE_TABLE.items())
    return f"{metric}{{{labels}}}"


@pytest.fixture(scope="module")
def table_stats(stack: Stack) -> Iterator[Stack]:
    """The smoke table, then the exporter measuring its namespace every 5 seconds through the
    REST catalog, scraped by Prometheus."""
    returncode, output = stack.run_app(SMOKE_APP)
    assert returncode == 0, output
    start_stage_exporter(replace(stack, env={**stack.env, **FAST_TABLE_STATS}, running=[]))
    start_prometheus(stack)
    try:
        wait_until(
            lambda: bool(query(stack, selector("iceberg_table_data_files"))),
            timeout=180,
            message="Prometheus never stored the smoke table's data files",
        )
        yield stack
    finally:
        remove_prometheus(stack)
        remove_stage_exporter(stack)


def test_the_table_files_bytes_and_records_are_measured(table_stats: Stack) -> None:
    measured = {
        metric: float(sample["value"][1])
        for metric in (
            "iceberg_table_data_files",
            "iceberg_table_data_bytes",
            "iceberg_table_records",
        )
        for sample in query(table_stats, selector(metric))
    }

    assert measured["iceberg_table_data_files"] >= 1
    assert measured["iceberg_table_data_bytes"] > 0
    assert measured["iceberg_table_records"] == 4


def test_the_smoke_tables_files_are_all_small(table_stats: Stack) -> None:
    [small] = query(table_stats, selector("iceberg_table_small_files"))
    [files] = query(table_stats, selector("iceberg_table_data_files"))

    assert small["value"][1] == files["value"][1]
    assert query(table_stats, selector("iceberg_partition_small_files"))
