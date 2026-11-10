import subprocess

from tests.infra.gold_fixtures import item_change, order_change, rows
from tests.infra.stack import REPO_ROOT, Stack, wait_until

RETAIN = "SELECT * FROM gold_internal.apply_history_retention(36, 2, current_date);"
DEFAULT_ROWS = "SELECT count(*) FROM gold_internal.fact_order_history_default;"
COMMITTED_ORDER = 990001
RETENTION_SERVICE = "gold-retention-job"


def in_month(offset: int) -> str:
    """A SQL timestamp a few days into the month `offset` months from the current one."""
    return f"date_trunc('month', localtimestamp) + make_interval(months => {offset}, days => 3)"


def suffix(stack: Stack, offset: int) -> str:
    """The partition name suffix (YYYY_MM) of the month `offset` months from the current one."""
    [[name]] = rows(
        stack,
        f"SELECT to_char(date_trunc('month', localtimestamp) + make_interval(months => {offset}),"
        " 'YYYY_MM');",
    )
    return name


def partitions(fact: str) -> str:
    return f"""
SELECT child.relname
FROM pg_inherits AS i
JOIN pg_class AS parent ON parent.oid = i.inhparent
JOIN pg_class AS child ON child.oid = i.inhrelid
WHERE parent.relname = '{fact}'
ORDER BY 1;
"""


def old_open_order(order_id: int, offset: int) -> list[str]:
    """An open order whose only change is `offset` months from now, in history and current state."""
    return [
        order_change(order_id, 10, in_month(offset)),
        "INSERT INTO gold_internal.current_orders (order_id, status_key, is_open, _commit_sequence,"
        f" _changed_at) VALUES ({order_id}, 1, true, 10, {in_month(offset)});",
    ]


def old_item(order_item_id: int, offset: int) -> str:
    return item_change(order_item_id, 10, in_month(offset), quantity=1)


def make_gold_retention(stack: Stack) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "gold-retention"],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def first_service_loop(stack: Stack) -> str:
    """The log of the scheduled retention service's first run."""

    def log() -> str:
        return stack.compose("logs", "--no-log-prefix", RETENTION_SERVICE).stdout

    try:
        stack.compose("--profile", "maintenance", "up", "--detach", "--no-build", RETENTION_SERVICE)
        wait_until(
            lambda: "Gold retention as of" in log(),
            timeout=120,
            message="the gold retention service never finished a run",
        )
        return log()
    finally:
        stack.compose("--profile", "maintenance", "rm", "-sf", RETENTION_SERVICE, check=False)


DROP_EMPTY_MONTHS = """
DO $$
DECLARE
    month record;
BEGIN
    FOR month IN
        SELECT child.relname
        FROM pg_inherits AS i
        JOIN pg_class AS parent ON parent.oid = i.inhparent
        JOIN pg_class AS child ON child.oid = i.inhrelid
        WHERE parent.relname IN ('fact_order_history', 'fact_order_item_history')
          AND child.relname NOT LIKE '%_default'
    LOOP
        EXECUTE format(
            'DO $drop$ BEGIN IF NOT EXISTS (SELECT FROM gold_internal.%1$I)'
            ' THEN DROP TABLE gold_internal.%1$I; END IF; END $drop$',
            month.relname
        );
    END LOOP;
END $$;
"""


def remove_committed_order(stack: Stack) -> None:
    stack.sql(
        "analytics",
        f"DELETE FROM gold_internal.fact_order_history WHERE order_id = {COMMITTED_ORDER};",
    )
    stack.sql("analytics", DROP_EMPTY_MONTHS)
