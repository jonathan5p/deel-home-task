import subprocess
from dataclasses import dataclass

from tests.infra.stack import REPO_ROOT, Stack

LANDING_ROOT = "s3a://lake/landing"

GOLD_CHECKPOINT = "checkpoints/gold/dim_customer/offsets/0"
SILVER_CHECKPOINT = "checkpoints/silver/customers/offsets/0"
CATALOG_TABLE = "gold_reset_keeps_this"
EXTRA_STATUS = 99
APPENDS_APP = "tests/infra/apps/small_appends.py"
LEGACY_VIEW_SQL = "tests/infra/sql/legacy_gold_view.sql"
LAKE_GOLD_TABLES = ("lake.gold_internal.dim_customer", "lake.gold.internal_dim_customer")
LAKE_GOLD_ENTRIES = """
SELECT count(*) FROM iceberg_tables
WHERE catalog_name = 'lake' AND table_namespace IN ('gold', 'gold_internal');
"""


@dataclass(frozen=True, slots=True)
class GoldReset:
    refused: subprocess.CompletedProcess[str]
    first: subprocess.CompletedProcess[str]
    second: subprocess.CompletedProcess[str]


def make_gold_reset(stack: Stack, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "gold-reset", *args],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        check=False,
    )


def leave_lake_gold(stack: Stack) -> None:
    """A gold table in gold_internal, one under its former name in gold, and a view over it."""
    for table in LAKE_GOLD_TABLES:
        code, output = stack.run_app(APPENDS_APP, table, "1")
        assert code == 0, output
    view = stack.run_sql_file(LEGACY_VIEW_SQL, LANDING_ROOT, {})
    assert view.returncode == 0, view.stdout + view.stderr


def lake_gold_entries(stack: Stack) -> str:
    return stack.sql("iceberg_catalog", LAKE_GOLD_ENTRIES)


def run_gold_reset_scenario(stack: Stack) -> GoldReset:
    """Leave gold data in PostgreSQL and the lake, gold and silver checkpoints and an Iceberg
    catalog table behind, then run `make gold-reset`: without CONFIRM, then twice with it."""
    leave_lake_gold(stack)
    stack.put_object(GOLD_CHECKPOINT, b"{}")
    stack.put_object(SILVER_CHECKPOINT, b"{}")
    stack.sql("iceberg_catalog", f"CREATE TABLE IF NOT EXISTS {CATALOG_TABLE} (x int);")
    stack.sql(
        "analytics",
        "INSERT INTO gold_internal.dim_order_status (status_key, status_code, is_open)"
        f" VALUES ({EXTRA_STATUS}, 'RESET_ME', true) ON CONFLICT DO NOTHING;",
    )
    refused = make_gold_reset(stack)
    first = make_gold_reset(stack, "CONFIRM=1")
    second = make_gold_reset(stack, "CONFIRM=1")
    return GoldReset(refused=refused, first=first, second=second)


def lake_has(stack: Stack, folder: str, name: str) -> bool:
    return name in stack.lake_entries(folder)


def remove_catalog_table(stack: Stack) -> None:
    stack.sql("iceberg_catalog", f"DROP TABLE IF EXISTS {CATALOG_TABLE};")
