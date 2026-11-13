import pytest

from analytics.pipelines.tests.lake.gold.postgres_setup import FakeConnection, StatementFailedError
from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.postgres import (
    COPY_LOCK,
    RETENTION_LOCK,
    Connection,
    connection_factory,
    conninfo,
    run_in_transaction,
    transaction,
)

SETTINGS = AnalyticsDbSettings(
    url="jdbc:postgresql://db:5432/analytics", user="analytics", password="secret"
)


def test_a_jdbc_url_becomes_a_libpq_connection_uri() -> None:
    assert conninfo("jdbc:postgresql://db:5432/analytics") == "postgresql://db:5432/analytics"


def test_a_libpq_connection_uri_is_kept() -> None:
    assert conninfo("postgresql://db/analytics") == "postgresql://db/analytics"


def test_the_factory_opens_a_new_connection_with_the_settings_each_time() -> None:
    calls: list[tuple[str, dict[str, str]]] = []

    def connect(uri: str, **credentials: str) -> Connection:
        calls.append((uri, credentials))
        return FakeConnection()

    factory = connection_factory(SETTINGS, connect)
    first, second = factory(), factory()

    assert first is not second
    assert calls == 2 * [
        ("postgresql://db:5432/analytics", {"user": "analytics", "password": "secret"})
    ]


def test_the_statements_run_in_order_then_commit_and_close() -> None:
    connection = FakeConnection()

    run_in_transaction(lambda: connection, ["first", "second"])

    assert connection.log == ["execute first", "execute second", "commit", "close"]


def test_a_failing_statement_rolls_back_the_transaction_and_is_raised() -> None:
    connection = FakeConnection(fail_on="second")

    with pytest.raises(StatementFailedError, match="second"):
        run_in_transaction(lambda: connection, ["first", "second", "third"])

    assert connection.log == ["execute first", "execute second", "rollback", "close"]


def test_a_transaction_commits_what_its_block_did_and_closes() -> None:
    connection = FakeConnection(results={"query": [("a",), ("b",)]})

    with transaction(lambda: connection) as open_connection:
        open_connection.execute("first")
        fetched = open_connection.execute("query").fetchall()

    assert fetched == [("a",), ("b",)]
    assert connection.log == ["execute first", "execute query", "commit", "close"]


def test_a_transaction_whose_block_fails_rolls_back_and_raises() -> None:
    connection = FakeConnection(fail_on="second")

    def write(open_connection: Connection) -> None:
        open_connection.execute("first")
        open_connection.execute("second")

    with pytest.raises(StatementFailedError), transaction(lambda: connection) as open_connection:
        write(open_connection)

    assert connection.log == ["execute first", "execute second", "rollback", "close"]


def test_copies_share_the_gold_lock_and_retention_takes_it_alone() -> None:
    assert COPY_LOCK == (
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('gold_internal', 0))"
    )
    assert RETENTION_LOCK == "SELECT pg_advisory_xact_lock(hashtextextended('gold_internal', 0))"
