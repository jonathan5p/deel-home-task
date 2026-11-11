from pathlib import Path

import pytest
from prometheus_client import CollectorRegistry

from analytics.pipelines.tests.observability.table_stats_setup import (
    SMALL,
    FakeTables,
    catalog,
    files,
)
from pipelines.observability.table_stats import (
    IcebergTables,
    TableMetrics,
    TableStats,
    TableStatsCollector,
    partition_name,
    small_files_by_partition,
    table_stats,
)

ORDERS = {"namespace": "bronze", "table": "orders"}


def collector(tables: FakeTables, registry: CollectorRegistry) -> TableStatsCollector:
    return TableStatsCollector(tables, TableMetrics(registry), ("bronze", "silver"), SMALL)


def test_table_stats_count_files_bytes_records_and_small_files() -> None:
    stats = table_stats(files(("a", 400), ("a", 1_000), ("b", 2_600)), small_file_bytes=SMALL)

    assert stats == TableStats(files=3, bytes=4_000, records=3, small_files=1)
    assert stats.average_file_bytes == pytest.approx(4_000 / 3)


def test_an_empty_table_has_no_average_file_size() -> None:
    assert table_stats([], small_file_bytes=SMALL).average_file_bytes == 0


def test_small_files_are_counted_per_partition_that_has_any() -> None:
    found = small_files_by_partition(
        files(("day=1", 10), ("day=1", 20), ("day=2", 5_000), ("day=3", 30)), small_file_bytes=SMALL
    )

    assert found == {"day=1": 2, "day=3": 1}


def test_a_partition_is_named_by_its_fields() -> None:
    assert partition_name({"day": "2026-11-11", "entity": 3}) == "day=2026-11-11/entity=3"
    assert partition_name({}) == ""


def test_the_table_gauges_follow_the_latest_collection() -> None:
    registry = CollectorRegistry()
    tables = FakeTables({("bronze", "orders"): files(("a", 10), ("a", 20))})
    stats = collector(tables, registry)

    stats.collect_tables()
    tables.by_table[("bronze", "orders")] = files(("a", 30))
    stats.collect_tables()

    assert registry.get_sample_value("iceberg_table_data_files", ORDERS) == 1
    assert registry.get_sample_value("iceberg_table_data_bytes", ORDERS) == 30
    assert registry.get_sample_value("iceberg_table_small_files", ORDERS) == 1
    assert registry.get_sample_value("iceberg_table_average_file_bytes", ORDERS) == 30


def test_a_table_that_disappears_loses_its_gauges() -> None:
    registry = CollectorRegistry()
    tables = FakeTables({("bronze", "orders"): files(("a", 10))})
    stats = collector(tables, registry)

    stats.collect_tables()
    tables.by_table.clear()
    stats.collect_tables()

    assert registry.get_sample_value("iceberg_table_data_files", ORDERS) is None


def test_a_partition_compaction_fixed_disappears_from_the_partition_gauge() -> None:
    registry = CollectorRegistry()
    tables = FakeTables({("bronze", "orders"): files(("day=1", 10), ("day=2", 10))})
    stats = collector(tables, registry)

    stats.collect_partitions()
    tables.by_table[("bronze", "orders")] = files(("day=1", 5_000), ("day=2", 10))
    stats.collect_partitions()

    day = ORDERS | {"partition": "day=1"}
    assert registry.get_sample_value("iceberg_partition_small_files", day) is None
    assert (
        registry.get_sample_value("iceberg_partition_small_files", ORDERS | {"partition": "day=2"})
        == 1
    )


def test_only_the_configured_namespaces_are_read() -> None:
    tables = FakeTables({("bronze", "orders"): files(), ("gold", "x"): files()})

    collector(tables, CollectorRegistry()).collect_tables()

    assert tables.reads == [("bronze", "orders")]


def test_iceberg_tables_lists_the_tables_of_existing_namespaces(tmp_path: Path) -> None:
    tables = IcebergTables(catalog(tmp_path))

    assert tables.tables(["bronze", "silver", "missing"]) == [
        ("bronze", "orders"),
        ("silver", "orders"),
    ]


def test_iceberg_tables_reads_the_current_data_files_with_their_partitions(tmp_path: Path) -> None:
    found = IcebergTables(catalog(tmp_path)).data_files("bronze", "orders")

    assert sorted((file.partition, file.records) for file in found) == [
        ("day=2026-11-10", 1),
        ("day=2026-11-10", 2),
        ("day=2026-11-11", 1),
    ]
    assert all(file.size > 0 for file in found)


def test_an_empty_table_has_no_data_files(tmp_path: Path) -> None:
    assert IcebergTables(catalog(tmp_path)).data_files("silver", "orders") == []
