import json
import subprocess

from tests.infra.stack import REPO_ROOT, Stack

GET = """
import sys, urllib.error, urllib.request
try:
    response = urllib.request.urlopen("http://localhost:8000" + sys.argv[1], timeout=30)
    status, body = response.status, response.read()
except urllib.error.HTTPError as error:
    status, body = error.code, error.read()
print(status)
print(body.decode())
"""


def start_api(stack: Stack) -> None:
    stack.compose("up", "--detach", "--build", "--wait", "api")


def get(stack: Stack, path: str) -> tuple[int, str]:
    """GET a path of the API from inside its own container, which has no host port here."""
    run = subprocess.run(
        ["docker", "compose", "exec", "-T", "api", "python", "-c", GET, path],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        check=True,
    )
    status, body = run.stdout.split("\n", 1)
    return int(status), body.strip()


def get_json(stack: Stack, path: str) -> tuple[int, object]:
    status, body = get(stack, path)
    return status, json.loads(body)


def as_psql_rows(rows: object) -> list[list[str]]:
    """JSON rows as `rows()` reads psql's output: every value as text, null as empty."""
    assert isinstance(rows, list)
    return [["" if value is None else str(value) for value in row.values()] for row in rows]


def health_status(stack: Stack) -> str:
    return stack.compose("ps", "--format", "{{.Health}}", "api").stdout.strip()
