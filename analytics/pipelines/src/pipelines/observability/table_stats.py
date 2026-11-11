from collections import Counter as Tally
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Gauge
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError

TABLE_LABELS = ("namespace", "table")
PARTITION_LABELS = (*TABLE_LABELS, "partition")


@dataclass(frozen=True, slots=True)
class DataFile:
    partition: str
    size: int
    records: int


@dataclass(frozen=True, slots=True)
class TableStats:
    files: int
    bytes: int
    records: int
    small_files: int

    @property
    def average_file_bytes(self) -> float:
        return self.bytes / self.files if self.files else 0.0


def table_stats(files: Sequence[DataFile], small_file_bytes: int) -> TableStats:
    return TableStats(
        files=len(files),
        bytes=sum(file.size for file in files),
        records=sum(file.records for file in files),
        small_files=sum(1 for file in files if file.size < small_file_bytes),
    )


def small_files_by_partition(files: Iterable[DataFile], small_file_bytes: int) -> dict[str, int]:
    """The partitions that hold small files, with how many each holds."""
    return dict(Tally(file.partition for file in files if file.size < small_file_bytes))


def partition_name(partition: Mapping[str, Any]) -> str:
    """A partition as `field=value` pairs, such as `_published_day=2026-11-11`; empty for an
    unpartitioned table."""
    return "/".join(f"{name}={value}" for name, value in partition.items())


class TableSource(Protocol):
    def tables(self, namespaces: Iterable[str]) -> list[tuple[str, str]]: ...

    def data_files(self, namespace: str, table: str) -> list[DataFile]: ...


@dataclass(frozen=True, slots=True)
class IcebergTables:
    """The tables of an Iceberg catalog and the data files of their current snapshots."""

    catalog: Catalog

    def tables(self, namespaces: Iterable[str]) -> list[tuple[str, str]]:
        """The tables in the namespaces; a namespace that does not exist yet has none."""
        found = []
        for namespace in namespaces:
            try:
                identifiers = self.catalog.list_tables(namespace)
            except NoSuchNamespaceError:
                continue
            found.extend((namespace, identifier[-1]) for identifier in identifiers)
        return sorted(found)

    def data_files(self, namespace: str, table: str) -> list[DataFile]:
        files = self.catalog.load_table((namespace, table)).inspect.data_files()
        return [
            DataFile(
                partition_name(row["partition"]), row["file_size_in_bytes"], row["record_count"]
            )
            for row in files.select(["partition", "file_size_in_bytes", "record_count"]).to_pylist()
        ]


@dataclass
class TableMetrics:
    """Gauges of the Iceberg tables' data files, labelled by namespace and table."""

    registry: CollectorRegistry

    def __post_init__(self) -> None:
        def gauge(name: str, documentation: str, labels: Sequence[str] = TABLE_LABELS) -> Gauge:
            return Gauge(name, documentation, labels, registry=self.registry)

        self.files = gauge("iceberg_table_data_files", "Data files in the current snapshot.")
        self.bytes = gauge(
            "iceberg_table_data_bytes", "Bytes of the current snapshot's data files."
        )
        self.records = gauge("iceberg_table_records", "Records in the current snapshot.")
        self.average = gauge(
            "iceberg_table_average_file_bytes", "Average size of the current snapshot's data files."
        )
        self.small = gauge(
            "iceberg_table_small_files", "Data files smaller than the small-file threshold."
        )
        self.partition_small = gauge(
            "iceberg_partition_small_files",
            "Data files smaller than the small-file threshold, per partition that has any.",
            PARTITION_LABELS,
        )

    def record_tables(self, stats: Mapping[tuple[str, str], TableStats]) -> None:
        for gauge in (self.files, self.bytes, self.records, self.average, self.small):
            gauge.clear()
        for (namespace, table), table_stats_ in stats.items():
            labels = {"namespace": namespace, "table": table}
            self.files.labels(**labels).set(table_stats_.files)
            self.bytes.labels(**labels).set(table_stats_.bytes)
            self.records.labels(**labels).set(table_stats_.records)
            self.average.labels(**labels).set(table_stats_.average_file_bytes)
            self.small.labels(**labels).set(table_stats_.small_files)

    def record_partitions(self, small_files: Mapping[tuple[str, str], Mapping[str, int]]) -> None:
        """Replace the partition gauges, so a partition that compaction fixed disappears."""
        self.partition_small.clear()
        for (namespace, table), partitions in small_files.items():
            for partition, count in partitions.items():
                self.partition_small.labels(namespace, table, partition).set(count)


@dataclass
class TableStatsCollector:
    """Reads the data files of every table in `namespaces` into the table metrics; the partition
    view reads them again, on its own, slower schedule."""

    source: TableSource
    metrics: TableMetrics
    namespaces: Sequence[str]
    small_file_bytes: int

    def collect_tables(self) -> int:
        stats = {
            (namespace, table): table_stats(
                self.source.data_files(namespace, table), self.small_file_bytes
            )
            for namespace, table in self.source.tables(self.namespaces)
        }
        self.metrics.record_tables(stats)
        return len(stats)

    def collect_partitions(self) -> int:
        small_files = {
            (namespace, table): small_files_by_partition(
                self.source.data_files(namespace, table), self.small_file_bytes
            )
            for namespace, table in self.source.tables(self.namespaces)
        }
        self.metrics.record_partitions(small_files)
        return len(small_files)
