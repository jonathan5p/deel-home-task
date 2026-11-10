import pytest

from pipelines.bronze.columns import RawColumn
from pipelines.lake.columns import MetadataColumn
from pipelines.schemas.definitions import load_entity, load_table_templates
from tests.infra.bronze_setup import (
    AUDIT_BRONZE,
    BRONZE_ROWS,
    ENTITIES,
    FAILURE_REASONS,
    FIRST_LSN,
    INVALID_RECORDS,
    LAKE_KEY,
    STRAY_OFFSET,
    TABLES,
    TOPICS,
    BronzeRuns,
    offsets,
    table_descriptions,
)
from tests.infra.stack import Stack


@pytest.mark.parametrize("entity", ENTITIES)
def test_each_entity_table_holds_typed_rows_with_their_change_metadata(
    bronze_runs: BronzeRuns, entity: str
) -> None:
    rows = bronze_runs.first[f"lake.bronze.{entity}"]

    assert [row[MetadataColumn.CHANGE_OP] for row in rows] == ["snapshot", "update"]
    for offset, row in enumerate(rows):
        assert {name: row[name] for name in BRONZE_ROWS[entity] | AUDIT_BRONZE} == (
            BRONZE_ROWS[entity] | AUDIT_BRONZE
        )
        assert row[MetadataColumn.RECORD_SOURCE] == TOPICS[entity]
        assert (row[MetadataColumn.SOURCE_PARTITION], row[MetadataColumn.SOURCE_OFFSET]) == (
            "0",
            str(offset),
        )
        assert row[MetadataColumn.PUBLISHED_AT] == "2026-11-08 14:21:01"
        assert row[MetadataColumn.COMMIT_SEQUENCE] == str(FIRST_LSN + offset)
        assert row[MetadataColumn.CHANGED_AT] == "2026-11-08 14:21:00.025"
        assert row[MetadataColumn.CAPTURED_AT] == "2026-11-08 14:21:00.561"
    hashes = {row[MetadataColumn.CONTENT_HASH] for row in rows}
    assert len(hashes) == 1
    assert len(hashes.pop() or "") == 64


def test_invalid_records_are_quarantined_while_valid_ones_flow(bronze_runs: BronzeRuns) -> None:
    for entity in ENTITIES:
        quarantine = bronze_runs.first[f"lake.bronze.{entity}_quarantine"]
        if entity in FAILURE_REASONS:
            (row,) = quarantine
            assert (row[MetadataColumn.SOURCE_OFFSET], row[MetadataColumn.FAILURE_REASON]) == (
                "2",
                FAILURE_REASONS[entity],
            )
            assert row[RawColumn.VALUE] == INVALID_RECORDS[entity]
            assert row[MetadataColumn.QUARANTINED_AT] is not None
        else:
            assert quarantine == []
        assert offsets(bronze_runs.first[f"lake.bronze.{entity}"]) == [0, 1]


def test_quarantine_rows_record_the_landing_file_they_were_read_from(
    stack: Stack, bronze_runs: BronzeRuns
) -> None:
    for entity in FAILURE_REASONS:
        folder = f"{LAKE_KEY}/landing/{TOPICS[entity]}/"
        (row,) = bronze_runs.first[f"lake.bronze.{entity}_quarantine"]
        landing_file = str(row[MetadataColumn.LANDING_FILE])

        assert landing_file.startswith(f"s3a://lake/{folder}part-")
        assert landing_file.endswith(".avro")
        assert landing_file.removeprefix(f"s3a://lake/{folder}") in stack.lake_entries(folder)
    for entity in ENTITIES:
        assert all(
            MetadataColumn.LANDING_FILE not in row
            for row in bronze_runs.first[f"lake.bronze.{entity}"]
        )


def test_tombstones_reach_neither_table(bronze_runs: BronzeRuns) -> None:
    assert 2 not in offsets(bronze_runs.first["lake.bronze.orders"])
    assert bronze_runs.first["lake.bronze.orders_quarantine"] == []


def test_bronze_data_files_are_partitioned_by_publish_day(
    stack: Stack, bronze_runs: BronzeRuns
) -> None:
    for table in ("customers", "customers_quarantine"):
        partitions = stack.lake_entries(f"warehouse/bronze/{table}/data/")
        assert partitions == {f"{MetadataColumn.PUBLISHED_AT}_day=2026-11-08"}


@pytest.mark.parametrize("entity", ENTITIES)
def test_a_restart_appends_only_the_records_landed_since(
    bronze_runs: BronzeRuns, entity: str
) -> None:
    rows = bronze_runs.restarted[f"lake.bronze.{entity}"]

    assert offsets(rows) == [0, 1, 3]
    assert rows[:2] == bronze_runs.first[f"lake.bronze.{entity}"]
    assert rows[2][MetadataColumn.CHANGE_OP] == "delete"
    assert (
        bronze_runs.restarted[f"lake.bronze.{entity}_quarantine"]
        == bronze_runs.first[f"lake.bronze.{entity}_quarantine"]
    )


def test_a_run_with_nothing_new_appends_nothing(bronze_runs: BronzeRuns) -> None:
    assert bronze_runs.idle == bronze_runs.restarted


def test_each_record_is_written_exactly_once_across_runs(bronze_runs: BronzeRuns) -> None:
    for table in TABLES:
        coordinates = [
            (
                row[MetadataColumn.RECORD_SOURCE],
                row[MetadataColumn.SOURCE_PARTITION],
                row[MetadataColumn.SOURCE_OFFSET],
            )
            for row in bronze_runs.idle[table]
        ]
        assert len(coordinates) == len(set(coordinates)), table


def test_files_outside_the_landing_commit_log_are_ignored(bronze_runs: BronzeRuns) -> None:
    assert STRAY_OFFSET not in offsets(bronze_runs.idle["lake.bronze.customers"])
    assert STRAY_OFFSET not in offsets(bronze_runs.idle["lake.bronze.customers_quarantine"])


def test_every_bronze_table_and_column_is_described(stack: Stack, bronze_runs: BronzeRuns) -> None:
    descriptions = table_descriptions(stack)

    assert sorted(description["table"] for description in descriptions) == sorted(TABLES)
    for description in descriptions:
        assert description["comment"], description["table"]
        undescribed = [name for name, doc in description["columns"].items() if not doc]
        assert undescribed == [], description["table"]
        assert description["columns"], description["table"]
    by_table = {description["table"]: description for description in descriptions}
    order_items = by_table["lake.bronze.order_items"]["columns"]
    (quanity,) = (c for c in load_entity("order_items").columns if c.name == "quanity")
    assert order_items["quantity"] == quanity.description
    (commit_sequence,) = (
        c
        for c in load_table_templates()["bronze"].fields
        if c.name == MetadataColumn.COMMIT_SEQUENCE
    )
    assert order_items[MetadataColumn.COMMIT_SEQUENCE] == commit_sequence.description
