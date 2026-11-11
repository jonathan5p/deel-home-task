import json
from collections.abc import Sequence
from typing import Any

from tests.infra.stack import Stack

READ_APP = "tests/infra/apps/lake_read.py"
ROWS_PREFIX = "TABLE ROWS "
DESCRIPTIONS_PREFIX = "TABLE DESCRIPTION "
CATALOG_URI = "http://iceberg-rest:8181"
S3_ENDPOINT = "seaweedfs:8333"

type Rows = list[dict[str, str | None]]
# The read apps print untyped JSON; each caller knows the shape of its dumps.
type Dump = dict[str, Any]


def dumps(output: str, prefix: str) -> list[Dump]:
    """The JSON dumps that a read app printed after `prefix`, one per line."""
    return [
        json.loads(line.removeprefix(prefix))
        for line in output.splitlines()
        if line.startswith(prefix)
    ]


def read_app(stack: Stack, *args: str) -> str:
    run = stack.python(READ_APP, *args)
    assert run.returncode == 0, run.stdout + run.stderr
    return run.stdout


def duckdb_rows(stack: Stack, tables: Sequence[str]) -> dict[str, Rows]:
    """Every row of each table, with every value as text in Spark's form, read with DuckDB
    through the REST catalog, or straight from the table's files when it is named by an s3://
    path."""
    if not all(table.startswith("s3://") for table in tables):
        stack.start_catalog()
    output = read_app(stack, "rows", CATALOG_URI, S3_ENDPOINT, *tables)
    return {dump["table"]: dump["rows"] for dump in dumps(output, ROWS_PREFIX)}


def catalog_descriptions(stack: Stack, tables: Sequence[str]) -> list[Dump]:
    """The comment and column docs of each table or view, as the REST catalog stores them."""
    stack.start_catalog()
    return dumps(read_app(stack, "descriptions", CATALOG_URI, *tables), DESCRIPTIONS_PREFIX)
