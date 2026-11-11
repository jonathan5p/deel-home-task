import pytest

pytest.importorskip("pyspark")

import duckdb

from analytics.pipelines.tests.latency.landing_samples import (
    FILE_A,
    FILE_B,
    FILE_C,
    FakeLogStore,
    envelope,
    log_text,
    records_table,
)
from analytics.pipelines.tests.latency.latency_setup import CUSTOMER
from pipelines.latency.landing import (
    LandingWatch,
    LogEntry,
    SinkLog,
    SinkLogError,
    avro_files,
    batch_id,
    change_of,
    committed_files,
    current_logs,
    landing_match_query,
    parse_sink_log,
)
from pipelines.latency.probe import Change
from pipelines.latency.sql import Row


def test_a_sink_log_lists_its_files_after_the_version_header() -> None:
    text = log_text(FILE_A) + "\n\n" + log_text(FILE_B).removeprefix("v1\n") + "\n"

    assert parse_sink_log("3", text) == [LogEntry(FILE_A, "add"), LogEntry(FILE_B, "add")]


def test_a_sink_log_of_another_version_is_rejected() -> None:
    with pytest.raises(SinkLogError, match="'v2'"):
        parse_sink_log("3", "v2\n{}")


def test_a_sink_log_line_must_name_a_file() -> None:
    with pytest.raises(SinkLogError, match="not a file entry"):
        parse_sink_log("3", 'v1\n{"size": 1}')


@pytest.mark.parametrize(
    ("name", "expected"),
    [("0", 0), ("12", 12), ("19.compact", 19), (".12.tmp", None), ("12.crc", None)],
)
def test_the_batch_of_a_log_file_is_its_number(name: str, expected: int | None) -> None:
    assert batch_id(name) == expected


def test_the_current_logs_are_the_latest_compact_log_and_the_batches_after_it() -> None:
    names = ["8", "10", "9.compact", "11", "x", "19.compact", "20", "21"]

    assert current_logs(names) == ["19.compact", "20", "21"]


def test_without_a_compact_log_every_batch_log_is_current_in_batch_order() -> None:
    assert current_logs(["10", "2", "1"]) == ["1", "2", "10"]


def test_the_committed_files_apply_the_logs_in_order() -> None:
    logs = [[LogEntry(FILE_A, "add"), LogEntry(FILE_B, "add")], [LogEntry(FILE_A, "delete")]]

    assert committed_files(logs) == {FILE_B}


def test_the_sink_log_reports_only_the_files_committed_since_the_mark() -> None:
    store = FakeLogStore({"0": log_text(FILE_A)})
    log = SinkLog(store)
    log.mark()

    store.logs["1"] = log_text(FILE_B)
    first = log.new_files()
    store.logs["2.compact"] = log_text(FILE_A, FILE_B, FILE_C)
    second = log.new_files()

    assert (first, second, log.new_files()) == ([FILE_B], [FILE_C], [])


def test_the_sink_log_reads_each_log_file_once() -> None:
    store = FakeLogStore({"0": log_text(FILE_A)})
    log = SinkLog(store)

    log.mark()
    log.new_files()

    assert store.reads == ["0"]


def run(connection: duckdb.DuckDBPyConnection, sql: str) -> list[Row]:
    return connection.execute(sql).fetchall()


def test_the_match_query_finds_the_targets_change_with_the_marker() -> None:
    connection = duckdb.connect()
    records = records_table(
        connection,
        [
            None,
            envelope(7, -1, lsn=10),
            envelope(8, -42, lsn=20),
            envelope(7, -42, lsn=30, ts_ms=1500, captured=1600),
        ],
    )

    rows = run(connection, landing_match_query(records, CUSTOMER, -42))

    assert rows == [(30, 1500, 1600, 5000)]


def test_the_match_query_finds_nothing_without_the_marker() -> None:
    connection = duckdb.connect()
    records = records_table(connection, [envelope(7, -1, lsn=10)])

    assert run(connection, landing_match_query(records, CUSTOMER, -42)) == []


def test_a_matched_row_becomes_the_change_and_no_row_none() -> None:
    assert change_of([(30, 1500, 1600, 5000)]) == Change(30, 1500, 1600, 5000)
    assert change_of([]) is None


def test_landed_files_are_read_as_avro_through_s3() -> None:
    assert avro_files([FILE_A, "/local/file.avro"]) == (
        "read_avro(['s3://bucket/landing/topic/part-a.avro', '/local/file.avro'])"
    )


def test_the_watch_searches_only_the_files_landed_since_its_mark() -> None:
    store = FakeLogStore({"0": log_text(FILE_A)})
    queries: list[str] = []

    def query(sql: str) -> list[Row]:
        queries.append(sql)
        return [(30, 1500, 1600, 5000)]

    watch = LandingWatch(SinkLog(store), query, records=lambda files: "|".join(files))
    watch.mark()
    nothing_new = watch.find(CUSTOMER, -42)
    store.logs["1"] = log_text(FILE_B)
    found = watch.find(CUSTOMER, -42)

    assert nothing_new is None
    assert found == Change(30, 1500, 1600, 5000)
    assert queries == [landing_match_query(FILE_B, CUSTOMER, -42)]
