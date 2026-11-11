import json
import os
import sys

import duckdb

PREFIX = "TABLE COUNTS "


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main(catalog_uri: str, s3_endpoint: str, tables: list[str]) -> None:
    """Attach the lake through its REST catalog with DuckDB and print each table's row count as
    one JSON line."""
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
    connection.execute(
        f"ATTACH 'warehouse' AS lake (TYPE iceberg, URI {literal(catalog_uri)},"
        " AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
    )
    counts: dict[str, int] = {}
    for table in tables:
        row = connection.execute(f"SELECT count(*) FROM {table}").fetchone()
        assert row is not None
        counts[table] = int(row[0])
    print(f"{PREFIX}{json.dumps(counts)}", flush=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
