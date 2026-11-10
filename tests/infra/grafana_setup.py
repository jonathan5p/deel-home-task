import json
import subprocess
from typing import Any

from tests.infra.gold_fixtures import psql
from tests.infra.stack import REPO_ROOT, Stack

GRAFANA = "grafana"
API = "http://localhost:3000"
REQUEST = 'wget -qO- --header "Content-Type: application/json" {post}"{url}$API_PATH"'
ADMIN = "http://$GF_SECURITY_ADMIN_USER:$GF_SECURITY_ADMIN_PASSWORD@localhost:3000"
AS_GRAFANA_READER = (
    'PGPASSWORD="$GRAFANA_READER_PASSWORD" psql --no-psqlrc --quiet --no-align --tuples-only'
    ' --set=ON_ERROR_STOP=1 --host=localhost --username="$GRAFANA_READER_USER"'
    ' --dbname="$POSTGRES_DB" --file=-'
)
BREAK_GRAFANA_READER = """
ALTER ROLE grafana NOLOGIN PASSWORD 'something-else';
ALTER ROLE grafana RESET ALL;
REVOKE gold_reader FROM grafana;
"""

# Grafana's API answers with untyped JSON: an object or a list, depending on the endpoint.
type Json = dict[str, Any]
type Response = Json | list[Any]


def start_grafana(stack: Stack) -> None:
    stack.compose("up", "--detach", "--wait", GRAFANA)


def request(stack: Stack, path: str, *, body: Json | None = None, admin: bool = True) -> Response:
    """Grafana's JSON answer, called as the admin or anonymously."""
    post = '--post-data "$BODY" ' if body is not None else ""
    script = REQUEST.format(post=post, url=ADMIN if admin else API)
    run = stack.compose(
        "exec",
        "-T",
        "-e",
        f"API_PATH={path}",
        "-e",
        f"BODY={json.dumps(body or {})}",
        GRAFANA,
        "sh",
        "-c",
        script,
        check=False,
    )
    assert run.returncode == 0, f"{path}: {run.stderr}"
    return json.loads(run.stdout)


def as_grafana_reader(stack: Stack, script: str) -> subprocess.CompletedProcess[str]:
    """Run a SQL script in the analytics database as Grafana's service login."""
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "analytics-db", "sh", "-c", AS_GRAFANA_READER],
        cwd=REPO_ROOT,
        env=stack.env,
        input=script,
        capture_output=True,
        text=True,
        check=False,
    )


def break_grafana_reader(stack: Stack) -> None:
    """Take away the login, its password, its defaults and its membership of gold_reader."""
    run = psql(stack, BREAK_GRAFANA_READER)
    assert run.returncode == 0, run.stderr
