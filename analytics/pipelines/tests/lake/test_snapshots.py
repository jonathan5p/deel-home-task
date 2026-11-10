import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from pipelines.lake.checkpoints import CommittedBatch
from pipelines.lake.snapshots import Snapshot, committed_snapshot, read_snapshot, table_snapshots

QUERY = "query-a"
COMMITTED = CommittedBatch(query_id=QUERY, batch=4, sources=())


def appended(snapshot_id: int, epoch_id: int, query_id: str = QUERY) -> Snapshot:
    return Snapshot(snapshot_id, query_id, epoch_id)


def compacted(snapshot_id: int) -> Snapshot:
    return Snapshot(snapshot_id, None, None)


def test_the_newest_snapshot_of_a_committed_batch_bounds_the_table() -> None:
    snapshots = [appended(1, 3), appended(2, 4)]

    assert committed_snapshot(snapshots, COMMITTED) == 2


def test_a_batch_appended_before_the_checkpoint_committed_it_is_left_out() -> None:
    snapshots = [appended(1, 3), appended(2, 4), appended(3, 5)]

    assert committed_snapshot(snapshots, COMMITTED) == 2


def test_a_compaction_counts_until_a_later_batch_precedes_it() -> None:
    assert committed_snapshot([appended(1, 4), compacted(2)], COMMITTED) == 2
    assert committed_snapshot([appended(1, 4), appended(2, 5), compacted(3)], COMMITTED) == 1


def test_appends_by_other_writers_count_as_they_hold_no_batch_of_the_query() -> None:
    snapshots = [appended(1, 4), appended(2, 9, query_id="replay"), Snapshot(3, None, None)]

    assert committed_snapshot(snapshots, COMMITTED) == 3


def test_an_earlier_run_of_the_query_bounds_nothing_from_a_new_one() -> None:
    restarted = CommittedBatch(query_id="query-b", batch=0, sources=())

    assert committed_snapshot([appended(1, 7)], restarted) == 1


def test_a_table_without_snapshots_before_the_batch_has_none() -> None:
    assert committed_snapshot([], COMMITTED) is None
    assert committed_snapshot([appended(1, 5)], COMMITTED) is None


def test_the_snapshots_of_a_table_and_a_read_as_of_one(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.example.numbers"
    spark.createDataFrame([(1,)], "number BIGINT").writeTo(table).create()
    spark.createDataFrame([(2,)], "number BIGINT").writeTo(table).append()

    first, second = table_snapshots(spark, table)

    assert (first.query_id, first.epoch_id) == (None, None)
    assert second.snapshot_id != first.snapshot_id
    assert [row.number for row in read_snapshot(spark, table, first.snapshot_id).collect()] == [1]
