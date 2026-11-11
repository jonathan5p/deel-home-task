import pytest

pytest.importorskip("pyspark")

from collections.abc import Callable

import duckdb

from analytics.pipelines.tests.latency.latency_setup import CHANGE, CUSTOMER
from pipelines.latency.lake import LakeLookup, lake_lookup_query, lake_lookups, timestamp
from pipelines.latency.layers import Layer
from pipelines.latency.probe import Change
from pipelines.latency.sql import Row
from pipelines.schemas.definitions import load_table_templates

TEMPLATES = load_table_templates()


def lake(connection: duckdb.DuckDBPyConnection, partition: str) -> None:
    """An attached database named like the lake catalog, with a bronze customers table that
    holds the change once, at `partition` = its value."""
    connection.execute("ATTACH ':memory:' AS lake")
    connection.execute("CREATE SCHEMA lake.bronze")
    connection.execute(
        "CREATE TABLE lake.bronze.customers"
        f" (customer_id BIGINT, _commit_sequence BIGINT, {partition} TIMESTAMPTZ)"
    )
    connection.execute(
        f"INSERT INTO lake.bronze.customers VALUES (7, 500, {timestamp(CHANGE.published_at)})"
    )


def query(connection: duckdb.DuckDBPyConnection) -> Callable[[str], list[Row]]:
    return lambda sql: connection.execute(sql).fetchall()


def test_a_timestamp_literal_is_exact_whatever_the_session_time_zone() -> None:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'America/Bogota'")

    rows = connection.execute(f"SELECT epoch_ms({timestamp(1_794_426_435_489)})").fetchall()

    assert rows == [(1_794_426_435_489,)]


def test_the_lookup_filters_on_the_key_the_commit_sequence_and_the_partition_column() -> None:
    sql = lake_lookup_query('"lake"."bronze"."customers"', TEMPLATES["bronze"], CUSTOMER, CHANGE)

    assert sql == (
        'SELECT count(*) FROM "lake"."bronze"."customers" WHERE "customer_id" = 7'
        f' AND "_commit_sequence" = 500 AND "_published_at" = {timestamp(1_000_300)}'
    )


def test_a_template_without_a_partition_column_filters_on_the_key_only() -> None:
    sql = lake_lookup_query("t", TEMPLATES["silver_current"], CUSTOMER, CHANGE)

    assert sql.endswith('"customer_id" = 7 AND "_commit_sequence" = 500')


def test_the_bronze_lookup_sees_the_change_once_its_table_holds_it() -> None:
    connection = duckdb.connect()
    lake(connection, "_published_at")
    bronze = lake_lookups("lake", query(connection))[Layer.BRONZE]
    other = Change(501, CHANGE.changed_at, CHANGE.captured_at, CHANGE.published_at)

    assert bronze.table(CUSTOMER) == '"lake"."bronze"."customers"'
    assert bronze.visible(CUSTOMER, CHANGE)
    assert not bronze.visible(CUSTOMER, other)


def test_the_silver_lookup_reads_the_silver_table_by_its_partition_column() -> None:
    statements: list[str] = []

    def record(sql: str) -> list[Row]:
        statements.append(sql)
        return [(0,)]

    silver = LakeLookup("lake", TEMPLATES["silver"], record)

    assert not silver.visible(CUSTOMER, CHANGE)
    assert statements[0].startswith('SELECT count(*) FROM "lake"."silver"."customers"')
    assert f'"_changed_at" = {timestamp(CHANGE.changed_at)}' in statements[0]
