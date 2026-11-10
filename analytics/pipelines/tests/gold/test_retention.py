from datetime import date

import pytest

from analytics.pipelines.tests.gold.postgres_setup import FakeConnection, StatementFailedError
from analytics.pipelines.tests.gold.retention_setup import AS_OF, DATABASE, QUERY, SETTINGS
from pipelines.gold.postgres import WRITER_LOCK, Connection
from pipelines.gold.retention import apply_retention, main, retention_query


def test_the_query_applies_the_window_and_months_ahead_as_of_a_date() -> None:
    assert retention_query(SETTINGS, date(2026, 11, 10)) == QUERY


def test_retention_takes_the_gold_writer_lock_first_and_commits() -> None:
    connection = FakeConnection()

    apply_retention(lambda: connection, SETTINGS, AS_OF)

    assert connection.log == [f"execute {WRITER_LOCK}", f"execute {QUERY}", "commit", "close"]


def test_retention_returns_the_partitions_it_dropped() -> None:
    dropped: list[tuple[object, ...]] = [
        ("gold_internal.fact_order_history_2023_09",),
        ("gold_internal.x_2023_10",),
    ]
    connection = FakeConnection(results={QUERY: dropped})

    names = apply_retention(lambda: connection, SETTINGS, AS_OF)

    assert names == ["gold_internal.fact_order_history_2023_09", "gold_internal.x_2023_10"]


def test_a_failed_retention_is_rolled_back_and_raised() -> None:
    connection = FakeConnection(fail_on=QUERY)

    with pytest.raises(StatementFailedError):
        apply_retention(lambda: connection, SETTINGS, AS_OF)

    assert connection.log[-2:] == ["rollback", "close"]


def test_main_applies_retention_with_the_settings_from_the_environment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = FakeConnection(results={QUERY: [("gold_internal.fact_order_history_2023_09",)]})
    uris: list[str] = []

    def connect(uri: str, **credentials: str) -> Connection:
        uris.append(uri)
        return connection

    main(DATABASE | {"GOLD_RETENTION_MONTHS": "36"}, AS_OF, connect)

    assert uris == ["postgresql://db:5432/analytics"]
    assert f"execute {QUERY}" in connection.log
    assert capsys.readouterr().out.splitlines() == [
        "Gold retention as of 2026-11-10, keeping 36 months: dropped 1 partition(s)",
        "  gold_internal.fact_order_history_2023_09",
    ]


def test_main_reports_when_nothing_was_outside_the_window(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(DATABASE, AS_OF, lambda uri, **credentials: FakeConnection())

    assert capsys.readouterr().out.splitlines() == [
        "Gold retention as of 2026-11-10, keeping 36 months: dropped 0 partition(s)",
    ]
