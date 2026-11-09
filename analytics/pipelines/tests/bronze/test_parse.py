from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from analytics.pipelines.tests.bronze.parse_samples import (
    CONTENT_CHANGES,
    ENTITY_IDS,
    LSN,
    OVERFLOWING_PRICE,
    PUBLISHED,
    SAMPLES,
    Json,
    business,
    change,
    envelope,
    expected_bronze,
    landing,
    landing_file,
    landing_records,
    only_row,
    parse,
    source_row,
    without,
)
from pipelines.bronze.columns import (
    BRONZE_METADATA_COLUMNS,
    CAPTURED_AT,
    CHANGE_OP,
    CHANGED_AT,
    COMMIT_SEQUENCE,
    CONTENT_HASH,
    FAILURE_REASON,
    LANDING_FILE,
    PUBLISHED_AT,
    QUARANTINE_COLUMNS,
    RAW_KEY,
    RAW_VALUE,
    RECORD_SOURCE,
    SOURCE_OFFSET,
    SOURCE_PARTITION,
)
from pipelines.bronze.entities import CUSTOMERS, ENTITIES, ORDER_ITEMS, ORDERS, PRODUCTS, Entity
from pipelines.bronze.parse import (
    FailureReason,
    parse_changes,
)


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
    assert row[CHANGE_OP] == change_op
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

    assert [row[SOURCE_OFFSET] for row in parsed.bronze.collect()] == [0]
    assert parsed.quarantine.count() == 0


def test_bronze_rows_carry_the_source_coordinates_and_change_metadata(
    spark: SparkSession,
) -> None:
    records = landing_records(spark, landing(CUSTOMERS, change(CUSTOMERS, "c"), offset=41))

    row = only_row(parse_changes(records, CUSTOMERS).bronze)

    assert row[RECORD_SOURCE] == "test.operations.customers"
    assert row[SOURCE_PARTITION] == 0
    assert row[SOURCE_OFFSET] == 41
    assert row[PUBLISHED_AT] == PUBLISHED
    assert row[COMMIT_SEQUENCE] == LSN
    assert row[CHANGED_AT] == datetime(2026, 11, 8, 14, 21, 0, 25000)
    assert row[CAPTURED_AT] == datetime(2026, 11, 8, 14, 21, 0, 561000)


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
    assert types["customers"][COMMIT_SEQUENCE] == LongType()
    assert types["customers"][CHANGED_AT] == TimestampType()
    assert types["customers"][CONTENT_HASH] == StringType()


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

    assert len({row[CONTENT_HASH] for row in parsed.bronze.collect()}) == 1


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_a_content_change_changes_the_hash(spark: SparkSession, entity: Entity) -> None:
    changed = source_row(entity, **CONTENT_CHANGES[entity.name])

    parsed = parse(spark, entity, change(entity, "c"), change(entity, "u", changed))

    first, second = (row[CONTENT_HASH] for row in parsed.bronze.collect())
    assert first != second


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
    assert row[FAILURE_REASON] == reason
    assert row[LANDING_FILE] == landing_file(entity)
    assert parsed.bronze.count() == 0


def test_quarantine_rows_keep_the_raw_record_and_its_coordinates(spark: SparkSession) -> None:
    parsed = parse(spark, ORDERS, change(ORDERS, "c"), "not json")

    row = only_row(parsed.quarantine)
    assert row.asDict() == {
        RECORD_SOURCE: "test.operations.orders",
        SOURCE_PARTITION: 0,
        SOURCE_OFFSET: 1,
        PUBLISHED_AT: PUBLISHED,
        LANDING_FILE: "s3a://lake/landing/test.operations.orders/part-00000.avro",
        RAW_KEY: '{"order_id": 1}',
        RAW_VALUE: "not json",
        FAILURE_REASON: str(FailureReason.NOT_AN_ENVELOPE),
    }
    assert [row[SOURCE_OFFSET] for row in parsed.bronze.collect()] == [0]


def test_the_first_failed_check_is_the_reason(spark: SparkSession) -> None:
    row_without_key_or_name = without(source_row(CUSTOMERS, customer_name=None), "customer_id")

    parsed = parse(spark, CUSTOMERS, envelope("c", after=row_without_key_or_name))

    assert (
        only_row(parsed.quarantine)[FAILURE_REASON] == f"{FailureReason.MISSING_KEY}: customer_id"
    )
