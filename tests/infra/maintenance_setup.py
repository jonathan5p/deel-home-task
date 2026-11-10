import json
import subprocess
from typing import NamedTuple, TypedDict

from tests.infra.stack import REPO_ROOT, Stack, wait_until

NAMESPACE = "maintenance_test"
TABLE = f"lake.{NAMESPACE}.events"
COMMITS = 6
APPENDS_APP = "tests/infra/apps/small_appends.py"
STATS_APP = "tests/infra/apps/table_stats.py"
STATS_PREFIX = "TABLE STATS "
ENV = {"MAINTENANCE_NAMESPACES": f"{NAMESPACE},missing_namespace"}


class TableStats(TypedDict):
    data_files: int
    snapshots: list[str]
    ids: list[int]


class MaintenanceRun(NamedTuple):
    before: TableStats
    after: TableStats
    returncode: int
    output: str
    scheduled_output: str


def table_stats(stack: Stack) -> TableStats:
    code, output = stack.run_app(STATS_APP, TABLE)
    assert code == 0, output
    (line,) = [line for line in output.splitlines() if line.startswith(STATS_PREFIX)]
    stats: TableStats = json.loads(line.removeprefix(STATS_PREFIX))
    return stats


def lake_maintenance(stack: Stack) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "--no-print-directory", "lake-maintenance", "MAINTENANCE_UI_PORT=0"],
        cwd=REPO_ROOT,
        env={**stack.env, **ENV},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def compose(stack: Stack, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--profile", "maintenance", *args],
        cwd=REPO_ROOT,
        env={**stack.env, **ENV},
        capture_output=True,
        text=True,
        check=True,
    )


def scheduled_run_output(stack: Stack) -> str:
    """Start the maintenance service, wait for its first run to report the table, then stop it."""
    compose(stack, "up", "--detach", "maintenance-job")
    try:
        wait_until(
            lambda: (
                f"{TABLE.removeprefix('lake.')}:"
                in compose(stack, "logs", "maintenance-job").stdout
            ),
            timeout=300,
            message="the maintenance service never reported the test table",
        )
        return compose(stack, "logs", "maintenance-job").stdout
    finally:
        compose(stack, "rm", "--stop", "--force", "maintenance-job")


def run_maintenance_scenario(stack: Stack) -> MaintenanceRun:
    """Commit small appends to a test table, then run `make lake-maintenance` on its namespace."""
    code, output = stack.run_app(APPENDS_APP, TABLE, str(COMMITS))
    assert code == 0, output
    before = table_stats(stack)
    maintenance = lake_maintenance(stack)
    return MaintenanceRun(
        before=before,
        after=table_stats(stack),
        returncode=maintenance.returncode,
        output=maintenance.stdout + maintenance.stderr,
        scheduled_output=scheduled_run_output(stack),
    )
