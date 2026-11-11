import json
import subprocess
from dataclasses import dataclass
from typing import Any

from tests.infra.bronze_setup import ENTITIES, read_with_spark
from tests.infra.lake_reader import duckdb_rows
from tests.infra.stack import CATALOG_SERVICE, REPO_ROOT, Stack

READ_TABLES = tuple(
    f"lake.{layer}.{entity}" for layer in ("bronze", "silver") for entity in ENTITIES
)
WRITE_CHECK_NAMESPACE = "lake_catalog_write_check"

CATALOG_ENTRIES = """
SELECT table_namespace || '.' || table_name FROM iceberg_tables
WHERE catalog_name = 'lake' AND coalesce(iceberg_type, 'TABLE') = '{kind}'
"""
CATALOG_NAMESPACES = """
SELECT table_namespace FROM iceberg_tables WHERE catalog_name = 'lake'
UNION SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = 'lake'
"""
BREAK_CATALOG_READER = """
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM lake_catalog_reader;
ALTER ROLE lake_catalog_reader RESET ALL;
"""
AS_CATALOG_READER = (
    'PGPASSWORD="$LAKE_CATALOG_READER_PASSWORD" psql --no-psqlrc --quiet --no-align --tuples-only'
    ' --set=ON_ERROR_STOP=1 --host=localhost --username="$LAKE_CATALOG_READER_USER"'
    " --dbname=iceberg_catalog --file=-"
)

# The REST catalog answers with untyped JSON whose shape depends on the endpoint.
type Json = dict[str, Any]


@dataclass(frozen=True, slots=True)
class LakeCatalog:
    rest_namespaces: set[str]
    catalog_namespaces: set[str]
    rest_tables: set[str]
    catalog_tables: set[str]
    rest_views: set[str]
    catalog_views: set[str]
    duckdb_counts: dict[str, int]
    spark_counts: dict[str, int]


def rest(stack: Stack, path: str, *, body: Json | None = None) -> tuple[int, Json]:
    """Call the REST catalog from inside its own container; its status code and JSON answer."""
    post = ["-X", "POST", "-H", "Content-Type: application/json", "-d", json.dumps(body)]
    run = stack.compose(
        "exec",
        "-T",
        CATALOG_SERVICE,
        "curl",
        "-sS",
        "-w",
        "\n%{http_code}",
        *(post if body is not None else []),
        f"http://localhost:8181{path}",
    )
    answer, _, status = run.stdout.rpartition("\n")
    return int(status), json.loads(answer or "{}")


def rest_namespaces(stack: Stack) -> set[str]:
    status, answer = rest(stack, "/v1/namespaces")
    assert status == 200, answer
    return {".".join(namespace) for namespace in answer["namespaces"]}


def rest_identifiers(stack: Stack, namespaces: set[str], kind: str) -> set[str]:
    """Every table or view (`kind`) the REST catalog lists in the namespaces."""
    found: set[str] = set()
    for namespace in namespaces:
        status, answer = rest(stack, f"/v1/namespaces/{namespace}/{kind}")
        assert status == 200, answer
        found |= {f"{namespace}.{entry['name']}" for entry in answer["identifiers"]}
    return found


def catalog_rows(stack: Stack, query: str) -> set[str]:
    return set(stack.sql("iceberg_catalog", query).splitlines())


def catalog_namespaces(stack: Stack) -> set[str]:
    return catalog_rows(stack, CATALOG_NAMESPACES)


def duckdb_counts(stack: Stack) -> dict[str, int]:
    """Row counts of the read tables as DuckDB reads them through the REST catalog."""
    return {table: len(rows) for table, rows in duckdb_rows(stack, READ_TABLES).items()}


def spark_counts(stack: Stack) -> dict[str, int]:
    tables = read_with_spark(stack, READ_TABLES, lambda _: 0)
    return {table: len(rows) for table, rows in tables.items()}


def run_catalog_scenario(stack: Stack) -> LakeCatalog:
    """Start the REST catalog over the lake the earlier scenarios left, then list and read it
    through REST and through the catalog database."""
    stack.start_catalog()
    namespaces = rest_namespaces(stack)
    return LakeCatalog(
        rest_namespaces=namespaces,
        catalog_namespaces=catalog_namespaces(stack),
        rest_tables=rest_identifiers(stack, namespaces, "tables"),
        catalog_tables=catalog_rows(stack, CATALOG_ENTRIES.format(kind="TABLE")),
        rest_views=rest_identifiers(stack, namespaces, "views"),
        catalog_views=catalog_rows(stack, CATALOG_ENTRIES.format(kind="VIEW")),
        duckdb_counts=duckdb_counts(stack),
        spark_counts=spark_counts(stack),
    )


def as_catalog_reader(stack: Stack, script: str) -> subprocess.CompletedProcess[str]:
    """Run a SQL script in the catalog database as the REST catalog's login."""
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "analytics-db", "sh", "-c", AS_CATALOG_READER],
        cwd=REPO_ROOT,
        env=stack.env,
        input=script,
        capture_output=True,
        text=True,
        check=False,
    )


def break_catalog_reader(stack: Stack) -> None:
    """Take away the login's read grants and its read-only default."""
    stack.sql("iceberg_catalog", BREAK_CATALOG_READER)
