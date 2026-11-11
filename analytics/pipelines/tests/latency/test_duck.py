from pathlib import Path

import pytest

pytest.importorskip("pyspark")

import duckdb

from analytics.pipelines.tests.latency.latency_setup import ENV
from pipelines.latency.duck import (
    EXTENSIONS,
    DuckDbSinkLogStore,
    DuckDbTextStore,
    MissingExtensionError,
    catalog_attach_statement,
    connection_config,
    load_extensions,
    postgres_attach_statement,
    postgres_query_sql,
    postgres_uri,
    query_runner,
    s3_secret_statement,
    setup_statements,
)
from pipelines.latency.settings import LakeReadSettings, LatencySettings

LAKE_READ = LakeReadSettings.from_env(ENV)


def test_the_s3_secret_reaches_the_endpoint_with_the_lakes_keys() -> None:
    assert s3_secret_statement(LAKE_READ) == (
        "CREATE OR REPLACE SECRET lake_s3 (TYPE s3, KEY_ID 'access', SECRET 'secret',"
        " ENDPOINT 'storage:8333', URL_STYLE 'path', USE_SSL false, REGION 'region-1')"
    )


def test_the_rest_catalog_is_attached_under_the_lakes_catalog_name() -> None:
    assert catalog_attach_statement("lake", LAKE_READ) == (
        "ATTACH 'warehouse' AS \"lake\" (TYPE iceberg, URI 'http://catalog:8181',"
        " AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
    )


def test_the_postgres_login_is_added_to_the_uri_encoded() -> None:
    assert postgres_uri("postgresql://db:5432/gold", "owner", "p@ss word") == (
        "postgresql://db:5432/gold?user=owner&password=p%40ss%20word"
    )
    assert postgres_uri("postgresql://db/gold?sslmode=require", "a", "b") == (
        "postgresql://db/gold?sslmode=require&user=a&password=b"
    )


def test_only_the_source_database_is_attached_writable() -> None:
    assert postgres_attach_statement("db", "postgresql://h/d", read_only=True) == (
        "ATTACH 'postgresql://h/d' AS db (TYPE postgres, READ_ONLY)"
    )
    assert postgres_attach_statement("db", "postgresql://h/d", read_only=False) == (
        "ATTACH 'postgresql://h/d' AS db (TYPE postgres)"
    )


def test_the_connection_uses_the_installed_extensions_and_never_downloads_one() -> None:
    assert connection_config(LatencySettings.from_env(ENV)) == {
        "extension_directory": "/opt/extensions",
        "autoinstall_known_extensions": False,
    }


def test_a_missing_extension_fails_naming_it_and_the_directory(tmp_path: Path) -> None:
    connection = duckdb.connect(
        config={"extension_directory": str(tmp_path), "autoinstall_known_extensions": False}
    )

    with pytest.raises(MissingExtensionError) as error:
        load_extensions(connection, str(tmp_path))

    assert str(error.value) == (
        f"the DuckDB extension {EXTENSIONS[0]} is not installed in {tmp_path};"
        " rebuild the spark-app image"
    )


def test_the_setup_attaches_the_lake_and_both_databases_without_installing() -> None:
    statements = setup_statements(LatencySettings.from_env(ENV))

    assert not [s for s in statements if s.startswith(("INSTALL", "LOAD"))]
    assert statements[0].startswith("CREATE OR REPLACE SECRET lake_s3")
    assert statements[-3].startswith("ATTACH 'warehouse' AS \"catalog\"")
    assert statements[-2] == (
        "ATTACH 'postgresql://source:5432/source?user=writer&password=p%40ss%20word'"
        " AS source_db (TYPE postgres)"
    )
    assert statements[-1].endswith("AS analytics_db (TYPE postgres, READ_ONLY)")


def test_a_postgres_statement_is_passed_through_as_a_string() -> None:
    assert postgres_query_sql("db", "SELECT 'a'") == (
        "SELECT * FROM postgres_query('db', 'SELECT ''a''')"
    )


def test_the_query_runner_returns_every_row() -> None:
    run = query_runner(duckdb.connect())

    assert run("SELECT * FROM range(2)") == [(0,), (1,)]


def test_text_is_written_as_is_and_replaced(tmp_path: Path) -> None:
    store = DuckDbTextStore(duckdb.connect())
    path = tmp_path / "report.html"
    text = '<p class="a">x, "y"\n\tz</p>\n'

    store.write_text(str(path), "first\n")
    store.write_text(str(path), text)

    assert path.read_text() == text
    assert store.read_text(str(path)) == text


def test_the_store_lists_the_files_with_a_suffix_directly_in_a_folder(tmp_path: Path) -> None:
    for name in ("b.json", "a.json", "a.html", "nested/c.json"):
        (tmp_path / name).parent.mkdir(exist_ok=True)
        (tmp_path / name).write_text("{}")
    store = DuckDbTextStore(duckdb.connect())

    assert store.list_files(str(tmp_path), ".json") == [
        f"{tmp_path}/a.json",
        f"{tmp_path}/b.json",
    ]
    assert store.list_files(str(tmp_path / "missing"), ".json") == []


def test_the_sink_log_store_reads_the_spark_metadata_folder(tmp_path: Path) -> None:
    folder = tmp_path / "_spark_metadata"
    folder.mkdir()
    (folder / "0").write_text("v1\n")
    (folder / "1.compact").write_text("v1\n{}")
    logs = DuckDbSinkLogStore(DuckDbTextStore(duckdb.connect()), str(tmp_path))

    assert logs.log_names() == ["0", "1.compact"]
    assert logs.read_log("1.compact") == "v1\n{}"
