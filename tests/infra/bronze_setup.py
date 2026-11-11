import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypedDict

from pipelines.lake.columns import MetadataColumn
from tests.infra.lake_reader import ROWS_PREFIX, catalog_descriptions, duckdb_rows, dumps
from tests.infra.landing_avro import landing_avro
from tests.infra.stack import Stack

LAKE_KEY = "test-fixtures/bronze"
STAGING_KEY = f"{LAKE_KEY}/staging"
LAND_APP = "tests/infra/apps/land_staged.py"
BRONZE_APP = "analytics/pipelines/src/pipelines/bronze/job.py"
SPARK_ROWS_APP = "tests/infra/apps/table_rows.py"

ENTITIES = ("customers", "products", "orders", "order_items")
TOPICS = {entity: f"test.operations.{entity}" for entity in ENTITIES}
KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}
ENV = {
    "KAFKA_TOPICS": ",".join(TOPICS.values()),
    "LAKE_ROOT": f"s3a://lake/{LAKE_KEY}",
    "CHECKPOINT_ROOT": f"s3a://lake/{LAKE_KEY}/checkpoints",
}
TABLES = [f"lake.bronze.{entity}" for entity in ENTITIES] + [
    f"lake.bronze.{entity}_quarantine" for entity in ENTITIES
]

PUBLISHED = datetime(2026, 11, 8, 14, 21, 1, tzinfo=UTC)
SOURCE_TS_MS = 1794147660025
CAPTURED_TS_MS = 1794147660561
FIRST_LSN = 100
STRAY_OFFSET = 99

type Json = dict[str, object]
type Rows = list[dict[str, str | None]]
type Tables = dict[str, Rows]

AUDIT: Json = {
    "updated_at": 1794147660022,
    "updated_by": -1,
    "created_at": 1794147633457,
    "created_by": -1,
}
SOURCE_ROWS: dict[str, Json] = {
    "customers": {
        "customer_id": 7,
        "customer_name": "ACME INC",
        "is_active": True,
        "customer_address": "Blue Sky Street, 1234",
    },
    "products": {
        "product_id": 3,
        "product_name": "Notebook",
        "barcode": "7701234567890",
        "unity_price": {"scale": 2, "value": "AJk="},
        "is_active": True,
    },
    "orders": {
        "order_id": 1,
        "order_date": 20765,
        "delivery_date": 20775,
        "customer_id": 7,
        "status": "PENDING",
    },
    "order_items": {"order_item_id": 11, "order_id": 1, "product_id": 3, "quanity": 2},
}
AUDIT_BRONZE = {
    "updated_at": "2026-11-08 14:21:00.022",
    "updated_by": "-1",
    "created_at": "2026-11-08 14:20:33.457",
    "created_by": "-1",
}
BRONZE_ROWS: dict[str, dict[str, str]] = {
    "customers": {
        "customer_id": "7",
        "customer_name": "ACME INC",
        "is_active": "true",
        "customer_address": "Blue Sky Street, 1234",
    },
    "products": {
        "product_id": "3",
        "product_name": "Notebook",
        "barcode": "7701234567890",
        "unity_price": "1.5300000000",
        "is_active": "true",
    },
    "orders": {
        "order_id": "1",
        "order_date": "2026-11-08",
        "delivery_date": "2026-11-18",
        "customer_id": "7",
        "status": "PENDING",
    },
    "order_items": {"order_item_id": "11", "order_id": "1", "product_id": "3", "quantity": "2"},
}


def envelope(op: str, *, before: Json | None = None, after: Json | None = None, lsn: int) -> str:
    return json.dumps(
        {
            "before": before,
            "after": after,
            "source": {"lsn": lsn, "ts_ms": SOURCE_TS_MS},
            "op": op,
            "ts_ms": CAPTURED_TS_MS,
        }
    )


def source_row(entity: str, **overrides: object) -> Json:
    return SOURCE_ROWS[entity] | AUDIT | overrides


def record(entity: str, offset: int, value: str | None) -> Json:
    return {
        "key": json.dumps({KEYS[entity]: SOURCE_ROWS[entity][KEYS[entity]]}).encode(),
        "value": None if value is None else value.encode(),
        "topic": TOPICS[entity],
        "partition": 0,
        "offset": offset,
        "timestamp": PUBLISHED,
        "timestampType": 0,
    }


def change(entity: str, offset: int, op: str, *, lsn: int | None = None) -> Json:
    """A change at `offset`, committed at `lsn` (by default `FIRST_LSN + offset`)."""
    lsn = FIRST_LSN + offset if lsn is None else lsn
    if op == "d":
        return record(entity, offset, envelope(op, before=source_row(entity), lsn=lsn))
    return record(entity, offset, envelope(op, after=source_row(entity), lsn=lsn))


INVALID_RECORDS: dict[str, str | None] = {
    "customers": "not json",
    "products": envelope(
        "c", after=source_row("products", unity_price={"scale": 2, "value": "!!"}), lsn=102
    ),
    "orders": None,
    "order_items": envelope("t", lsn=102),
}
FAILURE_REASONS = {
    "customers": "value is not a JSON Debezium envelope",
    "products": "undecodable or overflowing decimal: unity_price",
    "order_items": "unknown op",
}


def first_batch(entity: str) -> list[Json]:
    """A snapshot read, an update, then a record that is invalid or a tombstone (orders)."""
    return [
        change(entity, 0, "r"),
        change(entity, 1, "u"),
        record(entity, 2, INVALID_RECORDS[entity]),
    ]


def second_batch(entity: str) -> list[Json]:
    return [change(entity, 3, "d")]


@dataclass(frozen=True, slots=True)
class BronzeRuns:
    first: Tables
    restarted: Tables
    idle: Tables


def offset(row: dict[str, str | None]) -> int:
    return int(str(row[MetadataColumn.SOURCE_OFFSET]))


def submit(stack: Stack, app: str, *args: str) -> str:
    returncode, output = stack.run_app(app, *args, env=ENV)
    assert returncode == 0, output
    return output


def land(stack: Stack, batch: str, records: dict[str, list[Json]]) -> None:
    """Stage records as Avro files, then land them through the landing job's file sink."""
    for entity, entity_records in records.items():
        stack.put_object(
            f"{STAGING_KEY}/{TOPICS[entity]}/{batch}.avro", landing_avro(entity_records)
        )
    submit(stack, LAND_APP, f"s3a://lake/{STAGING_KEY}")


def read_tables(
    stack: Stack, tables: Sequence[str], order_by: Callable[[dict[str, str | None]], int]
) -> Tables:
    """Every row of each table, with every value as text, sorted by `order_by`."""
    return {table: sorted(rows, key=order_by) for table, rows in duckdb_rows(stack, tables).items()}


def read_with_spark(
    stack: Stack, tables: Sequence[str], order_by: Callable[[dict[str, str | None]], int]
) -> Tables:
    """`read_tables` through a Spark driver, which also reads the Spark SQL views."""
    output = submit(stack, SPARK_ROWS_APP, *tables)
    read = dumps(output, ROWS_PREFIX)
    return {dump["table"]: sorted(dump["rows"], key=order_by) for dump in read}


def run_bronze_and_read_tables(stack: Stack) -> Tables:
    submit(stack, BRONZE_APP, "--available-now")
    return read_tables(stack, TABLES, offset)


def offsets(rows: Rows) -> list[int]:
    return [offset(row) for row in rows]


class StoredDescription(TypedDict):
    table: str
    comment: str | None
    columns: dict[str, str | None]


def table_descriptions(stack: Stack, tables: Sequence[str] = TABLES) -> list[StoredDescription]:
    """The comment and column docs of each table (every bronze table by default), as the catalog
    stores them."""
    return [
        StoredDescription(table=dump["table"], comment=dump["comment"], columns=dump["columns"])
        for dump in catalog_descriptions(stack, tables)
    ]


def run_bronze_scenario(stack: Stack) -> BronzeRuns:
    """Land and process two batches, then run once more with nothing new to process."""
    land(stack, "batch-1", {entity: first_batch(entity) for entity in ENTITIES})
    first = run_bronze_and_read_tables(stack)

    stray = [change("customers", STRAY_OFFSET, "c")]
    stack.put_object(
        f"{LAKE_KEY}/landing/{TOPICS['customers']}/part-stray.avro", landing_avro(stray)
    )
    land(stack, "batch-2", {entity: second_batch(entity) for entity in ENTITIES})
    restarted = run_bronze_and_read_tables(stack)

    idle = run_bronze_and_read_tables(stack)
    return BronzeRuns(first=first, restarted=restarted, idle=idle)
