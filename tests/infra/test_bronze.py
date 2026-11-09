import json
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from tests.infra.landing_avro import landing_avro
from tests.infra.stack import Stack

LAKE_KEY = "test-fixtures/bronze"
STAGING_KEY = f"{LAKE_KEY}/staging"
LAND_APP = "tests/infra/apps/land_staged.py"
BRONZE_APP = "analytics/pipelines/src/pipelines/bronze/job.py"
ROWS_APP = "tests/infra/apps/table_rows.py"
ROWS_PREFIX = "TABLE ROWS "

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


def change(entity: str, offset: int, op: str) -> Json:
    lsn = FIRST_LSN + offset
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
    return int(str(row["source_offset"]))


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


def run_bronze_and_read_tables(stack: Stack) -> Tables:
    submit(stack, BRONZE_APP, "--available-now")
    output = submit(stack, ROWS_APP, *TABLES)
    tables: Tables = {}
    for line in output.splitlines():
        if line.startswith(ROWS_PREFIX):
            dump = json.loads(line.removeprefix(ROWS_PREFIX))
            tables[dump["table"]] = sorted(dump["rows"], key=offset)
    return tables


@pytest.fixture(scope="module")
def runs(stack: Stack) -> BronzeRuns:
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


def offsets(rows: Rows) -> list[int]:
    return [offset(row) for row in rows]


@pytest.mark.parametrize("entity", ENTITIES)
def test_each_entity_table_holds_typed_rows_with_their_change_metadata(
    runs: BronzeRuns, entity: str
) -> None:
    rows = runs.first[f"lake.bronze.{entity}"]

    assert [row["change_op"] for row in rows] == ["snapshot", "update"]
    for offset, row in enumerate(rows):
        assert {name: row[name] for name in BRONZE_ROWS[entity] | AUDIT_BRONZE} == (
            BRONZE_ROWS[entity] | AUDIT_BRONZE
        )
        assert row["record_source"] == TOPICS[entity]
        assert (row["source_partition"], row["source_offset"]) == ("0", str(offset))
        assert row["published_at"] == "2026-11-08 14:21:01"
        assert row["change_position"] == str(FIRST_LSN + offset)
        assert row["changed_at"] == "2026-11-08 14:21:00.025"
        assert row["captured_at"] == "2026-11-08 14:21:00.561"
    hashes = {row["content_hash"] for row in rows}
    assert len(hashes) == 1
    assert len(hashes.pop() or "") == 64


def test_invalid_records_are_quarantined_while_valid_ones_flow(runs: BronzeRuns) -> None:
    for entity in ENTITIES:
        quarantine = runs.first[f"lake.bronze.{entity}_quarantine"]
        if entity in FAILURE_REASONS:
            (row,) = quarantine
            assert (row["source_offset"], row["failure_reason"]) == ("2", FAILURE_REASONS[entity])
            assert row["value"] == INVALID_RECORDS[entity]
            assert row["quarantined_at"] is not None
        else:
            assert quarantine == []
        assert offsets(runs.first[f"lake.bronze.{entity}"]) == [0, 1]


def test_quarantine_rows_record_the_landing_file_they_were_read_from(
    stack: Stack, runs: BronzeRuns
) -> None:
    for entity in FAILURE_REASONS:
        folder = f"{LAKE_KEY}/landing/{TOPICS[entity]}/"
        (row,) = runs.first[f"lake.bronze.{entity}_quarantine"]
        landing_file = str(row["landing_file"])

        assert landing_file.startswith(f"s3a://lake/{folder}part-")
        assert landing_file.endswith(".avro")
        assert landing_file.removeprefix(f"s3a://lake/{folder}") in stack.lake_entries(folder)
    for entity in ENTITIES:
        assert all("landing_file" not in row for row in runs.first[f"lake.bronze.{entity}"])


def test_tombstones_reach_neither_table(runs: BronzeRuns) -> None:
    assert 2 not in offsets(runs.first["lake.bronze.orders"])
    assert runs.first["lake.bronze.orders_quarantine"] == []


def test_bronze_data_files_are_partitioned_by_publish_day(stack: Stack, runs: BronzeRuns) -> None:
    for table in ("customers", "customers_quarantine"):
        partitions = stack.lake_entries(f"warehouse/bronze/{table}/data/")
        assert partitions == {"published_at_day=2026-11-08"}


@pytest.mark.parametrize("entity", ENTITIES)
def test_a_restart_appends_only_the_records_landed_since(runs: BronzeRuns, entity: str) -> None:
    rows = runs.restarted[f"lake.bronze.{entity}"]

    assert offsets(rows) == [0, 1, 3]
    assert rows[:2] == runs.first[f"lake.bronze.{entity}"]
    assert rows[2]["change_op"] == "delete"
    assert (
        runs.restarted[f"lake.bronze.{entity}_quarantine"]
        == runs.first[f"lake.bronze.{entity}_quarantine"]
    )


def test_a_run_with_nothing_new_appends_nothing(runs: BronzeRuns) -> None:
    assert runs.idle == runs.restarted


def test_each_record_is_written_exactly_once_across_runs(runs: BronzeRuns) -> None:
    for table in TABLES:
        coordinates = [
            (row["record_source"], row["source_partition"], row["source_offset"])
            for row in runs.idle[table]
        ]
        assert len(coordinates) == len(set(coordinates)), table


def test_files_outside_the_landing_commit_log_are_ignored(runs: BronzeRuns) -> None:
    assert STRAY_OFFSET not in offsets(runs.idle["lake.bronze.customers"])
    assert STRAY_OFFSET not in offsets(runs.idle["lake.bronze.customers_quarantine"])
