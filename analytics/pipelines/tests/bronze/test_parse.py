import base64
import json
from datetime import date, datetime
from decimal import Decimal
from typing import NamedTuple

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from pipelines.bronze.entities import CUSTOMERS, ENTITIES, ORDER_ITEMS, ORDERS, PRODUCTS, Entity
from pipelines.bronze.parse import (
    BRONZE_METADATA_COLUMNS,
    QUARANTINE_COLUMNS,
    FailureReason,
    ParsedChanges,
    parse_changes,
)
from pipelines.schemas import KAFKA_RECORD_SCHEMA

PUBLISHED = datetime(2026, 11, 8, 14, 21, 1)  # Spark returns naive timestamps in UTC
LSN = 26767664
SOURCE_TS_MS = 1794147660025
CAPTURED_TS_MS = 1794147660561

type Json = dict[str, object]


class Sample(NamedTuple):
    row: Json
    bronze: dict[str, object]


AUDIT_JSON: Json = {
    "updated_at": 1794147660022,
    "updated_by": -1,
    "created_at": 1794147633457,
    "created_by": -1,
}
AUDIT_BRONZE: dict[str, object] = {
    "updated_at": datetime(2026, 11, 8, 14, 21, 0, 22000),
    "updated_by": -1,
    "created_at": datetime(2026, 11, 8, 14, 20, 33, 457000),
    "created_by": -1,
}
SAMPLES = {
    "customers": Sample(
        row={
            "customer_id": 7,
            "customer_name": "ACME INC",
            "is_active": True,
            "customer_address": "Blue Sky Street, 1234",
        },
        bronze={
            "customer_id": 7,
            "customer_name": "ACME INC",
            "is_active": True,
            "customer_address": "Blue Sky Street, 1234",
        },
    ),
    "products": Sample(
        row={
            "product_id": 3,
            "product_name": "Notebook",
            "barcode": "7701234567890",
            "unity_price": {"scale": 2, "value": "AJk="},
            "is_active": True,
        },
        bronze={
            "product_id": 3,
            "product_name": "Notebook",
            "barcode": "7701234567890",
            "unity_price": Decimal("1.53"),
            "is_active": True,
        },
    ),
    "orders": Sample(
        row={
            "order_id": 1,
            "order_date": 20765,
            "delivery_date": 20775,
            "customer_id": 7,
            "status": "PENDING",
        },
        bronze={
            "order_id": 1,
            "order_date": date(2026, 11, 8),
            "delivery_date": date(2026, 11, 18),
            "customer_id": 7,
            "status": "PENDING",
        },
    ),
    "order_items": Sample(
        row={"order_item_id": 11, "order_id": 1, "product_id": 3, "quanity": 2},
        bronze={"order_item_id": 11, "order_id": 1, "product_id": 3, "quantity": 2},
    ),
}
CONTENT_CHANGES: dict[str, Json] = {
    "customers": {"customer_address": "Rain Forest Street, 1234"},
    "products": {"unity_price": {"scale": 2, "value": "AJo="}},
    "orders": {"status": "REPROCESSING"},
    "order_items": {"quanity": 3},
}


def topic(entity: Entity) -> str:
    return f"test.operations.{entity.name}"


def source_row(entity: Entity, **overrides: object) -> Json:
    return SAMPLES[entity.name].row | AUDIT_JSON | overrides


def expected_bronze(entity: Entity) -> dict[str, object]:
    return SAMPLES[entity.name].bronze | AUDIT_BRONZE


def envelope(
    op: str | None,
    *,
    before: Json | None = None,
    after: Json | None = None,
    lsn: int = LSN,
) -> Json:
    return {
        "before": before,
        "after": after,
        "source": {"connector": "postgresql", "lsn": lsn, "ts_ms": SOURCE_TS_MS, "txId": 952},
        "op": op,
        "ts_ms": CAPTURED_TS_MS,
        "transaction": None,
    }


def change(entity: Entity, op: str, row: Json | None = None) -> Json:
    image = row if row is not None else source_row(entity)
    if op == "d":
        return envelope(op, before=image)
    if op == "u":
        return envelope(op, before=source_row(entity, updated_by=-2), after=image)
    return envelope(op, after=image)


def landing(entity: Entity, value: Json | str | None, *, offset: int = 0) -> Row:
    key = {entity.key: SAMPLES[entity.name].row[entity.key]}
    payload = value if value is None or isinstance(value, str) else json.dumps(value)
    return Row(
        key=json.dumps(key).encode(),
        value=None if payload is None else payload.encode(),
        topic=topic(entity),
        partition=0,
        offset=offset,
        timestamp=PUBLISHED,
        timestampType=0,
    )


def parse(spark: SparkSession, entity: Entity, *values: Json | str | None) -> ParsedChanges:
    rows = [landing(entity, value, offset=offset) for offset, value in enumerate(values)]
    return parse_changes(spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA), entity)


def business(row: Row, entity: Entity) -> dict[str, object]:
    return {name: row[name] for name in expected_bronze(entity)}


def only_row(records: DataFrame) -> Row:
    rows = records.collect()
    assert len(rows) == 1
    return rows[0]


ENTITY_IDS = [entity.name for entity in ENTITIES]


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
@pytest.mark.parametrize(
    ("op", "change_op"), [("c", "create"), ("u", "update"), ("r", "snapshot"), ("d", "delete")]
)
def test_every_op_becomes_one_typed_bronze_row(
    spark: SparkSession, entity: Entity, op: str, change_op: str
) -> None:
    parsed = parse(spark, entity, change(entity, op))

    row = only_row(parsed.bronze)
    assert business(row, entity) == expected_bronze(entity)
    assert row.change_op == change_op
    assert parsed.quarantine.count() == 0


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_a_delete_keeps_the_last_known_values_from_before(
    spark: SparkSession, entity: Entity
) -> None:
    delete = envelope("d", before=source_row(entity), after=None)

    row = only_row(parse(spark, entity, delete).bronze)

    assert business(row, entity) == expected_bronze(entity)


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_an_update_takes_its_values_from_after(spark: SparkSession, entity: Entity) -> None:
    update = envelope(
        "u",
        before=source_row(entity, **CONTENT_CHANGES[entity.name]),
        after=source_row(entity),
    )

    row = only_row(parse(spark, entity, update).bronze)

    assert business(row, entity) == expected_bronze(entity)


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_tombstones_are_dropped_not_quarantined(spark: SparkSession, entity: Entity) -> None:
    parsed = parse(spark, entity, change(entity, "d"), None)

    assert [row.source_offset for row in parsed.bronze.collect()] == [0]
    assert parsed.quarantine.count() == 0


def test_bronze_rows_carry_the_source_coordinates_and_change_metadata(
    spark: SparkSession,
) -> None:
    rows = [landing(CUSTOMERS, change(CUSTOMERS, "c"), offset=41)]
    records = spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA)

    row = only_row(parse_changes(records, CUSTOMERS).bronze)

    assert row.record_source == "test.operations.customers"
    assert row.source_partition == 0
    assert row.source_offset == 41
    assert row.published_at == PUBLISHED
    assert row.change_position == LSN
    assert row.changed_at == datetime(2026, 11, 8, 14, 21, 0, 25000)
    assert row.captured_at == datetime(2026, 11, 8, 14, 21, 0, 561000)


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_bronze_columns_are_the_business_columns_then_the_metadata(
    spark: SparkSession, entity: Entity
) -> None:
    parsed = parse(spark, entity)

    business_columns = [column.bronze_name for column in entity.columns]
    assert parsed.bronze.columns == [*business_columns, *BRONZE_METADATA_COLUMNS]
    assert tuple(parsed.quarantine.columns) == QUARANTINE_COLUMNS


def test_source_encodings_become_typed_columns(spark: SparkSession) -> None:
    types = {
        entity.name: {field.name: field.dataType for field in parse(spark, entity).bronze.schema}
        for entity in ENTITIES
    }

    assert types["customers"]["updated_at"] == TimestampType()
    assert types["customers"]["created_by"] == LongType()
    assert types["products"]["unity_price"] == DecimalType(38, 10)
    assert types["orders"]["order_date"] == DateType()
    assert types["order_items"]["quantity"] == IntegerType()
    assert types["customers"]["change_position"] == LongType()
    assert types["customers"]["changed_at"] == TimestampType()
    assert types["customers"]["content_hash"] == StringType()


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_the_content_hash_ignores_audit_columns_and_the_op(
    spark: SparkSession, entity: Entity
) -> None:
    audit_only = source_row(entity, updated_at=1794147999999, updated_by=-7)
    parsed = parse(
        spark,
        entity,
        change(entity, "r"),
        change(entity, "u", audit_only),
        change(entity, "d"),
    )

    assert len({row.content_hash for row in parsed.bronze.collect()}) == 1


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_a_content_change_changes_the_hash(spark: SparkSession, entity: Entity) -> None:
    changed = source_row(entity, **CONTENT_CHANGES[entity.name])

    parsed = parse(spark, entity, change(entity, "c"), change(entity, "u", changed))

    first, second = (row.content_hash for row in parsed.bronze.collect())
    assert first != second


def without(row: Json, column: str) -> Json:
    return {name: value for name, value in row.items() if name != column}


OVERFLOWING_PRICE = {"scale": 0, "value": base64.b64encode((2**64).to_bytes(9, "big")).decode()}


@pytest.mark.parametrize(
    ("entity", "value", "reason"),
    [
        pytest.param(CUSTOMERS, "not json", FailureReason.NOT_AN_ENVELOPE, id="not json"),
        pytest.param(CUSTOMERS, "[1, 2]", FailureReason.NOT_AN_ENVELOPE, id="json array"),
        pytest.param(
            CUSTOMERS,
            {"after": SAMPLES["customers"].row, "op": "c"},
            FailureReason.NOT_AN_ENVELOPE,
            id="no source",
        ),
        pytest.param(
            CUSTOMERS,
            envelope(None, after=source_row(CUSTOMERS)),
            FailureReason.NOT_AN_ENVELOPE,
            id="no op",
        ),
        pytest.param(
            ORDERS,
            envelope("t", after=source_row(ORDERS)),
            FailureReason.UNKNOWN_OP,
            id="unknown op",
        ),
        pytest.param(
            ORDERS,
            envelope("c", after=without(source_row(ORDERS), "order_id")),
            f"{FailureReason.MISSING_KEY}: order_id",
            id="missing key",
        ),
        pytest.param(
            ORDER_ITEMS,
            envelope("c", after=None),
            f"{FailureReason.MISSING_KEY}: order_item_id",
            id="no row image",
        ),
        pytest.param(
            CUSTOMERS,
            envelope("u", after=source_row(CUSTOMERS, customer_name=None)),
            f"{FailureReason.MISSING_NOT_NULL}: customer_name",
            id="missing not null",
        ),
        pytest.param(
            PRODUCTS,
            envelope("c", after=without(source_row(PRODUCTS), "unity_price")),
            f"{FailureReason.MISSING_NOT_NULL}: unity_price",
            id="missing not null decimal",
        ),
        pytest.param(
            PRODUCTS,
            envelope("c", after=source_row(PRODUCTS, unity_price=OVERFLOWING_PRICE)),
            f"{FailureReason.INVALID_DECIMAL}: unity_price",
            id="overflowing decimal",
        ),
        pytest.param(
            PRODUCTS,
            envelope("c", after=source_row(PRODUCTS, unity_price={"scale": 2, "value": "!!"})),
            f"{FailureReason.INVALID_DECIMAL}: unity_price",
            id="undecodable decimal",
        ),
    ],
)
def test_invalid_records_are_quarantined_with_their_reason(
    spark: SparkSession, entity: Entity, value: Json | str, reason: str
) -> None:
    parsed = parse(spark, entity, value)

    row = only_row(parsed.quarantine)
    assert row.failure_reason == reason
    assert parsed.bronze.count() == 0


def test_quarantine_rows_keep_the_raw_record_and_its_coordinates(spark: SparkSession) -> None:
    parsed = parse(spark, ORDERS, change(ORDERS, "c"), "not json")

    row = only_row(parsed.quarantine)
    assert row.asDict() == {
        "record_source": "test.operations.orders",
        "source_partition": 0,
        "source_offset": 1,
        "published_at": PUBLISHED,
        "key": '{"order_id": 1}',
        "value": "not json",
        "failure_reason": str(FailureReason.NOT_AN_ENVELOPE),
    }
    assert [row.source_offset for row in parsed.bronze.collect()] == [0]


def test_the_first_failed_check_is_the_reason(spark: SparkSession) -> None:
    row_without_key_or_name = without(source_row(CUSTOMERS, customer_name=None), "customer_id")

    parsed = parse(spark, CUSTOMERS, envelope("c", after=row_without_key_or_name))

    assert only_row(parsed.quarantine).failure_reason == f"{FailureReason.MISSING_KEY}: customer_id"
