import base64
import json
from datetime import date, datetime
from decimal import Decimal
from typing import NamedTuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.bronze.columns import MetadataColumn
from pipelines.bronze.entities import ENTITIES, Entity
from pipelines.bronze.parse import ParsedChanges, parse_changes
from pipelines.schemas import KAFKA_RECORD_SCHEMA

PUBLISHED = datetime(2026, 11, 8, 14, 21, 1)  # Spark returns naive timestamps in UTC
LSN = 26767664
SOURCE_TS_MS = 1794147660025
CAPTURED_TS_MS = 1794147660561
LANDING_SCHEMA = StructType(
    [*KAFKA_RECORD_SCHEMA.fields, StructField(MetadataColumn.LANDING_FILE, StringType())]
)

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
ENTITY_IDS = [entity.name for entity in ENTITIES]
OVERFLOWING_PRICE = {"scale": 0, "value": base64.b64encode((2**64).to_bytes(9, "big")).decode()}


def topic(entity: Entity) -> str:
    return f"test.operations.{entity.name}"


def landing_file(entity: Entity) -> str:
    return f"s3a://lake/landing/{topic(entity)}/part-00000.avro"


def source_row(entity: Entity, **overrides: object) -> Json:
    return SAMPLES[entity.name].row | AUDIT_JSON | overrides


def expected_bronze(entity: Entity) -> dict[str, object]:
    return SAMPLES[entity.name].bronze | AUDIT_BRONZE


def without(row: Json, column: str) -> Json:
    return {name: value for name, value in row.items() if name != column}


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
        **{MetadataColumn.LANDING_FILE: landing_file(entity)},
    )


def landing_records(spark: SparkSession, *rows: Row) -> DataFrame:
    return spark.createDataFrame(list(rows), LANDING_SCHEMA)


def parse(spark: SparkSession, entity: Entity, *values: Json | str | None) -> ParsedChanges:
    rows = [landing(entity, value, offset=offset) for offset, value in enumerate(values)]
    return parse_changes(landing_records(spark, *rows), entity)


def business(row: Row, entity: Entity) -> dict[str, object]:
    return {name: row[name] for name in expected_bronze(entity)}


def only_row(records: DataFrame) -> Row:
    rows = records.collect()
    assert len(rows) == 1
    return rows[0]
