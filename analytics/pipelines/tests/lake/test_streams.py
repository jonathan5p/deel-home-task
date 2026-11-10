from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryManager

from analytics.pipelines.tests.lake.streams_setup import (
    NUMBERS,
    ExampleSettings,
    FakeCatalog,
    failing,
    numbers,
    start,
)
from pipelines.lake.streams import (
    StreamsFailedError,
    TableStream,
    await_queries,
    start_streams,
    wants_available_now,
)
from pipelines.lake.tables import UndescribedColumnError, described_schema


def test_a_stream_is_named_after_its_layer_and_table_and_has_its_own_checkpoint(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = ExampleSettings(checkpoint_root=tmp_path)
    rows = numbers(spark, tmp_path / "numbers", 1)

    stream = TableStream.for_table(settings, "numbers", rows, NUMBERS)

    assert (stream.name, stream.table, stream.checkpoint) == (
        "example numbers",
        "catalog.example.numbers",
        f"{tmp_path}/example/numbers",
    )
    assert stream.rows is rows
    assert stream.definition is NUMBERS
    assert stream.table_schema() == described_schema(stream.table, rows.schema, NUMBERS)


def test_every_table_is_created_with_its_definition_then_every_stream_starts(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    settings = ExampleSettings(checkpoint_root=tmp_path)
    catalog = FakeCatalog(tmp_path / "tables")
    table_streams = [
        TableStream.for_table(settings, name, numbers(spark, tmp_path / name, 2), NUMBERS)
        for name in ("odd", "even")
    ]

    queries = start_streams(
        spark,
        table_streams,
        settings,
        create=catalog.create,
        sink=catalog.sink(),
        available_now=True,
    )
    await_queries(streams, queries, available_now=True)

    assert catalog.definitions == {
        "catalog.example.odd": NUMBERS,
        "catalog.example.even": NUMBERS,
    }
    assert [query.name for query in queries] == ["example odd", "example even"]
    for stream in table_streams:
        assert catalog.schemas[stream.table] == stream.table_schema()
        assert sorted(row.number for row in catalog.rows(spark, stream.table)) == [0, 1]
        assert (tmp_path / "example" / stream.table.rpartition(".")[2] / "commits").is_dir()


def test_no_table_is_created_when_any_stream_has_an_undescribed_column(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = ExampleSettings(checkpoint_root=tmp_path)
    catalog = FakeCatalog(tmp_path / "tables")
    rows = numbers(spark, tmp_path / "numbers", 1)
    table_streams = [
        TableStream.for_table(settings, "numbers", rows, NUMBERS),
        TableStream.for_table(settings, "labels", rows.selectExpr("'one' AS label"), NUMBERS),
    ]

    with pytest.raises(UndescribedColumnError, match="labels have no definition: label"):
        start_streams(
            spark,
            table_streams,
            settings,
            create=catalog.create,
            sink=catalog.sink(),
            available_now=True,
        )

    assert catalog.definitions == {}


def test_a_continuous_run_keeps_its_streams_running_on_the_processing_time_trigger(
    spark: SparkSession, tmp_path: Path
) -> None:
    settings = ExampleSettings(checkpoint_root=tmp_path)
    catalog = FakeCatalog(tmp_path / "tables")
    stream = TableStream.for_table(settings, "numbers", numbers(spark, tmp_path / "n", 1), NUMBERS)

    (query,) = start_streams(
        spark, [stream], settings, create=catalog.create, sink=catalog.sink(), available_now=False
    )
    try:
        assert query.isActive
        assert query.name == "example numbers"
    finally:
        query.stop()


def test_a_failed_stream_stops_the_running_ones_and_fails_the_job(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    running = start(numbers(spark, tmp_path / "ok", 1), "running", tmp_path, available_now=False)
    broken = failing(numbers(spark, tmp_path / "bad", 1))
    failed = start(broken, "broken", tmp_path, available_now=False)

    with pytest.raises(StreamsFailedError, match=r"failed: broken$") as raised:
        await_queries(streams, [running, failed], available_now=False)

    assert isinstance(raised.value.__cause__, StreamingQueryException)
    assert not running.isActive
    assert running.exception() is None


def test_a_stream_that_ends_stops_the_running_ones_without_failing(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    rows = numbers(spark, tmp_path / "numbers", 1)
    running = start(rows, "still_running", tmp_path, available_now=False)
    ended = start(rows, "ended", tmp_path, available_now=True)

    await_queries(streams, [running, ended], available_now=False)

    assert not running.isActive
    assert not ended.isActive


def test_an_available_now_run_finishes_every_stream_before_failing(
    spark: SparkSession, streams: StreamingQueryManager, tmp_path: Path
) -> None:
    broken = failing(numbers(spark, tmp_path / "bad", 1))
    failed = start(broken, "broken_first", tmp_path, available_now=True)
    slow = start(numbers(spark, tmp_path / "ok", 5), "slow", tmp_path, available_now=True)

    with pytest.raises(StreamsFailedError, match=r"failed: broken_first$"):
        await_queries(streams, [failed, slow], available_now=True)

    assert slow.exception() is None
    assert sorted(row.number for row in spark.table("slow").collect()) == [0, 1, 2, 3, 4]


@pytest.mark.parametrize(("argv", "expected"), [([], False), (["--available-now"], True)])
def test_available_now_is_requested_with_a_flag(argv: list[str], expected: bool) -> None:
    assert wants_available_now(argv, prog="example") is expected


def test_unknown_arguments_are_rejected() -> None:
    with pytest.raises(SystemExit):
        wants_available_now(["--once"], prog="example")
