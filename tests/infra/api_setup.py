import json
import subprocess

from tests.infra.gold_fixtures import CURRENT_STATE, psql
from tests.infra.stack import REPO_ROOT, Stack

ANSWERS_APP = "tests/infra/apps/api_answers.py"

REMOVE_CURRENT_STATE = """
DELETE FROM gold_internal.current_order_items WHERE order_item_id BETWEEN 1001 AND 1008;
DELETE FROM gold_internal.current_orders WHERE order_id BETWEEN 101 AND 110;
DELETE FROM gold_internal.dim_customer
WHERE (customer_id, _commit_sequence) IN ((1, 100), (1, 300), (2, 110), (3, 120));
"""


def run_sql(stack: Stack, script: str) -> None:
    run = psql(stack, script)
    assert run.returncode == 0, run.stderr


def add_current_state(stack: Stack) -> None:
    run_sql(stack, CURRENT_STATE)


def remove_current_state(stack: Stack) -> None:
    run_sql(stack, REMOVE_CURRENT_STATE)


def api_reader_credentials(stack: Stack) -> tuple[str, str]:
    """The API login as the analytics database was created with, from its container."""
    values = stack.compose(
        "exec",
        "-T",
        "analytics-db",
        "sh",
        "-c",
        'printf "%s\\n%s" "$API_READER_USER" "$API_READER_PASSWORD"',
    ).stdout.split("\n")
    return values[0], values[1]


def api_answers(stack: Stack) -> dict[str, dict[str, list[object]]]:
    """Run the repository as the API login in an app container and collect its answers."""
    user, password = api_reader_credentials(stack)
    run = subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-e",
            f"ANALYTICS_DB_USER={user}",
            "-e",
            f"ANALYTICS_DB_PASSWORD={password}",
            "spark-app",
            "python",
            ANSWERS_APP,
        ],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stderr
    answers: dict[str, dict[str, list[object]]] = json.loads(run.stdout.splitlines()[-1])
    return answers
