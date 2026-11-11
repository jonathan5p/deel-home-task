import pytest

from tests.infra.gold_reader_setup import make_db_logins
from tests.infra.lake_catalog_setup import (
    READ_TABLES,
    WRITE_CHECK_NAMESPACE,
    LakeCatalog,
    as_catalog_reader,
    break_catalog_reader,
    catalog_namespaces,
    rest,
    rest_identifiers,
)
from tests.infra.silver_setup import SILVER_TABLES, SILVER_VIEWS
from tests.infra.stack import Stack


def test_the_rest_catalog_lists_the_namespaces_of_the_catalog_database(
    lake_catalog: LakeCatalog,
) -> None:
    assert {"bronze", "silver", "gold"} <= lake_catalog.rest_namespaces
    assert lake_catalog.rest_namespaces == lake_catalog.catalog_namespaces


def test_the_rest_catalog_lists_the_tables_of_the_catalog_database(
    lake_catalog: LakeCatalog,
) -> None:
    assert {table.removeprefix("lake.") for table in READ_TABLES} <= lake_catalog.rest_tables
    assert lake_catalog.rest_tables == lake_catalog.catalog_tables


def test_the_rest_catalog_lists_the_silver_views(lake_catalog: LakeCatalog) -> None:
    silver_views = {view for view in lake_catalog.rest_views if view.startswith("silver.")}

    assert silver_views == {view.removeprefix("lake.") for view in SILVER_VIEWS}
    assert lake_catalog.rest_views == lake_catalog.catalog_views


def test_duckdb_reads_the_bronze_and_silver_tables_through_the_rest_catalog(
    lake_catalog: LakeCatalog,
) -> None:
    assert all(lake_catalog.spark_counts[table] > 0 for table in SILVER_TABLES)
    assert lake_catalog.duckdb_counts == lake_catalog.spark_counts


def test_the_rest_catalog_cannot_create_a_namespace(
    stack: Stack, lake_catalog: LakeCatalog
) -> None:
    status, _ = rest(stack, "/v1/namespaces", body={"namespace": [WRITE_CHECK_NAMESPACE]})

    assert status >= 400
    assert catalog_namespaces(stack) == lake_catalog.catalog_namespaces


def test_the_catalog_login_starts_read_only(stack: Stack) -> None:
    run = as_catalog_reader(stack, "SHOW default_transaction_read_only;")

    assert run.returncode == 0, run.stderr
    assert run.stdout.split() == ["on"]


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        (
            "INSERT INTO iceberg_namespace_properties VALUES ('lake', 'nope', 'exists', 'true');",
            "permission denied for table iceberg_namespace_properties",
        ),
        ("CREATE TABLE public.nope (x int);", "permission denied for schema public"),
    ],
)
def test_the_catalog_login_cannot_write_even_out_of_a_read_only_transaction(
    stack: Stack, statement: str, error: str
) -> None:
    run = as_catalog_reader(stack, f"SET default_transaction_read_only = off;\n{statement}")

    assert run.returncode != 0
    assert error in run.stderr


def test_make_db_logins_restores_the_catalog_login(stack: Stack, lake_catalog: LakeCatalog) -> None:
    break_catalog_reader(stack)
    broken, _ = rest(stack, "/v1/namespaces/bronze/tables")

    restored = make_db_logins(stack)
    tables = rest_identifiers(stack, {"bronze"}, "tables")
    run = as_catalog_reader(stack, "SHOW default_transaction_read_only;")

    assert broken >= 400
    assert restored.returncode == 0, restored.stderr
    assert tables == {table for table in lake_catalog.rest_tables if table.startswith("bronze.")}
    assert run.stdout.split() == ["on"]
