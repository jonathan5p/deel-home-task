import subprocess
from dataclasses import dataclass

from tests.infra.stack import REPO_ROOT, Stack

GOLD_CHECKPOINT = "checkpoints/gold/dim_customer/offsets/0"
SILVER_CHECKPOINT = "checkpoints/silver/customers/offsets/0"
CATALOG_TABLE = "gold_reset_keeps_this"
EXTRA_STATUS = 99


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


def run_gold_reset_scenario(stack: Stack) -> GoldReset:
    """Leave gold data, gold and silver checkpoints and an Iceberg catalog table behind, then
    run `make gold-reset`: without CONFIRM, then twice with it."""
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
