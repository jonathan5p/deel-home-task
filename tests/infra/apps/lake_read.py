import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import duckdb

ROWS_PREFIX = "TABLE ROWS "
DESCRIPTIONS_PREFIX = "TABLE DESCRIPTION "
TIMESTAMPS = frozenset({"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"})
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Iceberg REST metadata is untyped JSON whose shape differs between tables and views.
type Metadata = dict[str, Any]


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def spark_timestamp(micros: int) -> str:
    """A timestamp as Spark casts it to a string: in UTC, the fraction without trailing zeros."""
    moment = EPOCH + timedelta(microseconds=micros)
    fraction = f"{moment.microsecond:06d}".rstrip("0")
    whole = moment.strftime("%Y-%m-%d %H:%M:%S")
    return f"{whole}.{fraction}" if fraction else whole


def spark_text(kind: str, value: object) -> str | None:
    """A DuckDB value as Spark casts it to a string; timestamps arrive as epoch microseconds,
    because DuckDB returns zoned timestamps to Python only through pytz."""
    match value:
        case None:
            return None
        case int() if kind in TIMESTAMPS:
            return spark_timestamp(value)
        case bool():
            return "true" if value else "false"
        case Decimal():
            return format(value, "f")
        case bytes():
            return value.decode("utf-8", errors="replace")
        case datetime():
            raise TypeError(f"unexpected {kind} value {value!r}")
        case date():
            return value.isoformat()
        case int() | str():
            return str(value)
    raise TypeError(f"no Spark text form for {kind} value {value!r}")


def table_rows(connection: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, str | None]]:
    """Every row of `relation`, in table column order, with every value as Spark renders it."""
    columns = [
        (str(name), str(kind))
        for name, kind, *_ in connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    ]
    selected = ", ".join(
        f"epoch_us({quoted(name)})" if kind in TIMESTAMPS else quoted(name)
        for name, kind in columns
    )
    return [
        {name: spark_text(kind, value) for (name, kind), value in zip(columns, row, strict=True)}
        for row in connection.execute(f"SELECT {selected} FROM {relation}").fetchall()
    ]


def relation(table: str) -> str:
    """A path names a table outside the REST catalog, read straight from its metadata files."""
    return f"iceberg_scan({literal(table)})" if table.startswith("s3://") else table


def connect(catalog_uri: str, s3_endpoint: str, tables: Sequence[str]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(config={"extension_directory": "/tmp/duckdb-extensions"})
    connection.execute("INSTALL iceberg")
    connection.execute("LOAD iceberg")
    connection.execute(
        "CREATE SECRET lake_s3 (TYPE s3,"
        f" KEY_ID {literal(os.environ['LAKE_S3_ACCESS_KEY'])},"
        f" SECRET {literal(os.environ['LAKE_S3_SECRET_KEY'])},"
        f" ENDPOINT {literal(s3_endpoint)},"
        " URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"
    )
    if any(relation(table) == table for table in tables):
        connection.execute(
            f"ATTACH 'warehouse' AS lake (TYPE iceberg, URI {literal(catalog_uri)},"
            " AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
        )
    return connection


def print_rows(catalog_uri: str, s3_endpoint: str, tables: Sequence[str]) -> None:
    """Print every row of each table as one JSON line, as the Spark table-rows app does."""
    connection = connect(catalog_uri, s3_endpoint, tables)
    for table in tables:
        dump = {"table": table, "rows": table_rows(connection, relation(table))}
        print(f"{ROWS_PREFIX}{json.dumps(dump)}", flush=True)


def catalog_metadata(catalog_uri: str, table: str) -> Metadata:
    """The REST catalog's metadata of a table or view named `catalog.namespace.name`."""
    _, namespace, name = table.split(".")
    for kind in ("tables", "views"):
        url = f"{catalog_uri}/v1/namespaces/{namespace}/{kind}/{name}"
        try:
            with urllib.request.urlopen(url) as response:
                metadata: Metadata = json.load(response)["metadata"]
                return metadata
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    raise LookupError(f"{table} is neither a table nor a view of the catalog")


def description(table: str, metadata: Metadata) -> dict[str, object]:
    """The table comment and column docs, as Spark's DESCRIBE and schema report them."""
    if "current-schema-id" in metadata:
        schema_id = metadata["current-schema-id"]
    else:
        (version,) = (
            version
            for version in metadata["versions"]
            if version["version-id"] == metadata["current-version-id"]
        )
        schema_id = version["schema-id"]
    (schema,) = (schema for schema in metadata["schemas"] if schema["schema-id"] == schema_id)
    return {
        "table": table,
        "comment": metadata.get("properties", {}).get("comment"),
        "columns": {field["name"]: field.get("doc") for field in schema["fields"]},
    }


def print_descriptions(catalog_uri: str, tables: Sequence[str]) -> None:
    for table in tables:
        dump = description(table, catalog_metadata(catalog_uri, table))
        print(f"{DESCRIPTIONS_PREFIX}{json.dumps(dump)}", flush=True)


def main(argv: Sequence[str]) -> None:
    """`rows CATALOG_URI S3_ENDPOINT TABLE...` reads tables with DuckDB, through the REST catalog
    or from a path; `descriptions CATALOG_URI TABLE...` reads their docs from the REST catalog."""
    match argv:
        case ["rows", catalog_uri, s3_endpoint, *tables]:
            print_rows(catalog_uri, s3_endpoint, tables)
        case ["descriptions", catalog_uri, *tables]:
            print_descriptions(catalog_uri, tables)
        case _:
            raise SystemExit(f"usage: {main.__doc__}")


if __name__ == "__main__":
    main(sys.argv[1:])
