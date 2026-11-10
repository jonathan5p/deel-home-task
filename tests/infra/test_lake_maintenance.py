import pytest

from tests.infra.maintenance_setup import (
    COMMITS,
    TABLE,
    MaintenanceRun,
    run_maintenance_scenario,
)
from tests.infra.stack import Stack


@pytest.fixture(scope="module")
def maintained(stack: Stack) -> MaintenanceRun:
    return run_maintenance_scenario(stack)


def test_maintenance_succeeds_and_reports_the_table(maintained: MaintenanceRun) -> None:
    assert maintained.returncode == 0, maintained.output
    table = TABLE.removeprefix("lake.")
    assert f"{table}: rewrote {COMMITS} data files into 1" in maintained.output


def test_compaction_leaves_one_data_file_with_the_same_rows(maintained: MaintenanceRun) -> None:
    assert maintained.before["data_files"] == COMMITS
    assert maintained.after["data_files"] == 1
    assert maintained.after["ids"] == maintained.before["ids"] == list(range(COMMITS))


def test_compaction_only_adds_a_replace_snapshot(maintained: MaintenanceRun) -> None:
    assert maintained.after["snapshots"] == [*maintained.before["snapshots"], "replace"]


def test_the_scheduled_service_runs_the_same_maintenance(maintained: MaintenanceRun) -> None:
    table = TABLE.removeprefix("lake.")

    assert f"{table}: rewrote 0 data files into 0" in maintained.scheduled_output
