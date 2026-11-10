import subprocess

from tests.infra.gold_fixtures import psql
from tests.infra.stack import REPO_ROOT, Stack

AS_API_READER = (
    'PGPASSWORD="$API_READER_PASSWORD" psql --no-psqlrc --quiet --no-align --tuples-only'
    ' --set=ON_ERROR_STOP=1 --host=localhost --username="$API_READER_USER"'
    ' --dbname="$POSTGRES_DB" --file=-'
)

BREAK_API_READER = """
ALTER ROLE api_reader NOLOGIN PASSWORD 'something-else';
ALTER ROLE api_reader RESET ALL;
REVOKE gold_reader FROM api_reader;
"""


def as_api_reader(stack: Stack, script: str) -> subprocess.CompletedProcess[str]:
    """Run a SQL script in the analytics database as the API's service login."""
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "analytics-db", "sh", "-c", AS_API_READER],
        cwd=REPO_ROOT,
        env=stack.env,
        input=script,
        capture_output=True,
        text=True,
        check=False,
    )


def make_db_logins(stack: Stack) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "db-logins"],
        cwd=REPO_ROOT,
        env=stack.env,
        capture_output=True,
        text=True,
        check=False,
    )


def break_api_reader(stack: Stack) -> None:
    run = psql(stack, BREAK_API_READER)
    assert run.returncode == 0, run.stderr
