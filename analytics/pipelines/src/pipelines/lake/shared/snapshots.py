from collections.abc import Sequence
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.lake.shared.checkpoints import CommittedBatch

QUERY_ID = "spark.sql.streaming.queryId"
EPOCH_ID = "spark.sql.streaming.epochId"


class Snapshot(NamedTuple):
    """An Iceberg snapshot, with the streaming query and batch that wrote it, if one did."""

    snapshot_id: int
    query_id: str | None
    epoch_id: int | None


def table_snapshots(spark: SparkSession, table: str) -> list[Snapshot]:
    """The table's snapshots in commit order; Iceberg's streaming sink records the query and the
    batch of each append in the snapshot summary."""
    rows = (
        spark.table(f"{table}.snapshots")
        .orderBy("committed_at")
        .select("snapshot_id", F.col("summary")[QUERY_ID], F.col("summary")[EPOCH_ID])
        .collect()
    )
    return [
        Snapshot(
            snapshot_id=snapshot_id,
            query_id=query_id,
            epoch_id=None if epoch_id is None else int(epoch_id),
        )
        for snapshot_id, query_id, epoch_id in rows
    ]


def committed_snapshot(snapshots: Sequence[Snapshot], committed: CommittedBatch) -> int | None:
    """The newest snapshot that holds no batch the stream wrote after its committed one.

    A batch's append can land before the checkpoint commits it, so it must not count yet.
    Snapshots without a batch of this query, such as compactions or appends by other writers,
    count while no later batch precedes them. None when the table has no such snapshot.
    """
    chosen = None
    for snapshot in snapshots:
        later = snapshot.epoch_id is not None and snapshot.epoch_id > committed.batch
        if snapshot.query_id == committed.query_id and later:
            break
        chosen = snapshot.snapshot_id
    return chosen


def read_snapshot(spark: SparkSession, table: str, snapshot_id: int) -> DataFrame:
    return spark.read.option("versionAsOf", snapshot_id).table(table)
