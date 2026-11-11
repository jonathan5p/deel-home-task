import json
from dataclasses import dataclass, field

import duckdb

FILE_A = "s3a://bucket/landing/topic/part-a.avro"
FILE_B = "s3a://bucket/landing/topic/part-b.avro"
FILE_C = "s3a://bucket/landing/topic/part-c.avro"


def log_text(*paths: str, action: str = "add") -> str:
    entries = (
        json.dumps({"path": path, "size": 1, "isDir": False, "action": action}) for path in paths
    )
    return "\n".join(["v1", *entries])


@dataclass
class FakeLogStore:
    logs: dict[str, str] = field(default_factory=dict)
    reads: list[str] = field(default_factory=list)

    def log_names(self) -> list[str]:
        return list(self.logs)

    def read_log(self, name: str) -> str:
        self.reads.append(name)
        return self.logs[name]


def envelope(key: int, marker: int, *, lsn: int, ts_ms: int = 1000, captured: int = 1100) -> str:
    row = {"customer_id": key, "customer_name": "a", "updated_by": marker}
    return json.dumps(
        {
            "before": None,
            "after": row,
            "op": "u",
            "ts_ms": captured,
            "source": {"lsn": lsn, "ts_ms": ts_ms},
        }
    )


def records_table(connection: duckdb.DuckDBPyConnection, values: list[str | None]) -> str:
    """A table with the landed Kafka columns that matter, one record per value, published
    one second apart from epoch second 2."""
    connection.execute('CREATE TABLE records (value BLOB, "timestamp" TIMESTAMP)')
    for index, value in enumerate(values):
        connection.execute(
            "INSERT INTO records VALUES (?::BLOB, make_timestamp(?))",
            [value, (2 + index) * 1_000_000],
        )
    return "records"
