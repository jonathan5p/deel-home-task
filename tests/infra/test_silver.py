import pytest

from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.schemas.definitions import load_entity, load_table_templates
from tests.infra.bronze_setup import AUDIT_BRONZE, BRONZE_ROWS, ENTITIES, table_descriptions
from tests.infra.silver_setup import (
    SILVER_TABLES,
    SilverRuns,
    commit_sequence,
    commit_sequences,
    run_silver_without_bronze_tables,
    state_store_files,
)
from tests.infra.stack import Stack

SILVER = load_table_templates()["silver"]


@pytest.mark.parametrize("entity", ENTITIES)
def test_each_bronze_change_becomes_one_version_row(silver_runs: SilverRuns, entity: str) -> None:
    rows = silver_runs.first[f"lake.silver.{entity}"]
    changes = sorted(silver_runs.bronze[f"lake.bronze.{entity}"], key=commit_sequence)

    columns = [column.lake_name for column in SILVER.columns_for(load_entity(entity))]
    assert [list(row) for row in rows] == [columns] * len(changes)
    assert [row[MetadataColumn.CHANGE_OP] for row in rows] == [
        ChangeOp.SNAPSHOT,
        ChangeOp.UPDATE,
        ChangeOp.DELETE,
    ]
    assert [row[MetadataColumn.IS_DELETED] for row in rows] == ["false", "false", "true"]
    for row, change in zip(rows, changes, strict=True):
        copied = [name for name in columns if name != MetadataColumn.IS_DELETED]
        assert {name: row[name] for name in copied} == {name: change[name] for name in copied}


@pytest.mark.parametrize("entity", ENTITIES)
def test_a_delete_version_keeps_the_last_known_values(silver_runs: SilverRuns, entity: str) -> None:
    delete = silver_runs.first[f"lake.silver.{entity}"][-1]

    expected = BRONZE_ROWS[entity] | AUDIT_BRONZE
    assert delete[MetadataColumn.IS_DELETED] == "true"
    assert {name: delete[name] for name in expected} == expected


@pytest.mark.parametrize("entity", ENTITIES)
def test_a_restart_appends_only_the_changes_bronze_appended_since(
    silver_runs: SilverRuns, entity: str
) -> None:
    table = f"lake.silver.{entity}"
    rows = silver_runs.restarted[table]

    assert commit_sequences(rows) == [100, 101, 103, 104]
    assert rows[:3] == silver_runs.first[table]
    assert (rows[3][MetadataColumn.CHANGE_OP], rows[3][MetadataColumn.IS_DELETED]) == (
        ChangeOp.UPDATE,
        "false",
    )


@pytest.mark.parametrize("entity", ENTITIES)
def test_a_change_bronze_holds_twice_becomes_one_version_row(
    silver_runs: SilverRuns, entity: str
) -> None:
    bronze = silver_runs.bronze_extended[f"lake.bronze.{entity}"]
    silver = silver_runs.restarted[f"lake.silver.{entity}"]

    assert sorted(commit_sequences(bronze)) == [100, 101, 101, 103, 104, 104]
    assert commit_sequences(silver) == [100, 101, 103, 104]


@pytest.mark.parametrize("entity", ENTITIES)
def test_the_job_keeps_its_deduplication_state_in_rocksdb(
    stack: Stack, silver_runs: SilverRuns, entity: str
) -> None:
    files = state_store_files(stack, entity)

    assert any(name.endswith(".zip") for name in files)
    assert not any(name.endswith(".delta") for name in files)


def test_a_run_with_nothing_new_appends_nothing(silver_runs: SilverRuns) -> None:
    assert silver_runs.idle == silver_runs.restarted


def test_silver_data_files_are_partitioned_by_change_day(
    stack: Stack, silver_runs: SilverRuns
) -> None:
    for entity in ENTITIES:
        partitions = stack.lake_entries(f"warehouse/silver/{entity}/data/")
        assert partitions == {f"{MetadataColumn.CHANGED_AT}_day=2026-11-08"}, entity


def test_every_silver_table_and_column_is_described(stack: Stack, silver_runs: SilverRuns) -> None:
    descriptions = {
        description["table"]: description
        for description in table_descriptions(stack, SILVER_TABLES)
    }

    assert sorted(descriptions) == sorted(SILVER_TABLES)
    for entity in ENTITIES:
        description = descriptions[f"lake.silver.{entity}"]
        columns = SILVER.columns_for(load_entity(entity))
        assert description["comment"] == SILVER.describe(load_entity(entity))
        assert description["columns"] == {
            column.lake_name: column.description for column in columns
        }


def test_the_job_fails_while_a_bronze_table_is_missing(stack: Stack) -> None:
    returncode, output = run_silver_without_bronze_tables(stack)

    assert returncode != 0
    assert "TABLE_OR_VIEW_NOT_FOUND" in output
