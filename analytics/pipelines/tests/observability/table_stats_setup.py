from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from pipelines.observability.table_stats import DataFile

SMALL = 1_000
ROWS = pa.schema([pa.field("id", pa.int64(), nullable=False), ("day", pa.string())])
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True), NestedField(2, "day", StringType())
)
BY_DAY = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "day"))


def files(*sizes: tuple[str, int]) -> list[DataFile]:
    return [DataFile(partition, size, records=1) for partition, size in sizes]


def catalog(warehouse: Path) -> InMemoryCatalog:
    """A local catalog with `bronze.orders`, partitioned by day and written in three commits
    (two files on one day, one on another), and an empty `silver.orders`."""
    local = InMemoryCatalog("local", warehouse=warehouse.as_uri())
    local.create_namespace("bronze")
    local.create_namespace("silver")
    orders = local.create_table("bronze.orders", schema=SCHEMA, partition_spec=BY_DAY)
    for ids, day in (([1, 2], "2026-11-10"), ([3], "2026-11-10"), ([4], "2026-11-11")):
        orders.append(pa.table({"id": ids, "day": [day] * len(ids)}, schema=ROWS))
    local.create_table("silver.orders", schema=SCHEMA)
    return local


def commit_no_rows(local: InMemoryCatalog, identifier: str) -> None:
    """Create `identifier` and commit an append with no rows, as a streaming batch that
    quarantined nothing does: the table's current snapshot then has no manifests."""
    local.create_table(identifier, schema=SCHEMA).append(pa.Table.from_pylist([], schema=ROWS))


@dataclass
class FakeTables:
    """A table source with fixed data files that counts how often each table is read."""

    by_table: dict[tuple[str, str], list[DataFile]]
    unreadable: set[tuple[str, str]] = field(default_factory=set)
    reads: list[tuple[str, str]] = field(default_factory=list)

    def tables(self, namespaces: Iterable[str]) -> list[tuple[str, str]]:
        wanted = set(namespaces)
        return sorted(key for key in self.by_table if key[0] in wanted)

    def data_files(self, namespace: str, table: str) -> list[DataFile]:
        self.reads.append((namespace, table))
        if (namespace, table) in self.unreadable:
            raise OSError(f"{namespace}.{table} is unreadable")
        return self.by_table[(namespace, table)]
