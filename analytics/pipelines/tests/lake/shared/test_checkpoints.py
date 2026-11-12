from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.shared.checkpoints_setup import (
    CHECKPOINT,
    QUERY_ID,
    FakeCheckpoints,
    checkpoint,
    file_log,
    offsets,
    snapshot_offset,
)
from pipelines.lake.shared.checkpoints import (
    HadoopCheckpointFiles,
    committed_batch,
    committed_files,
    committed_snapshots,
)

SOURCES = f"{CHECKPOINT}/sources/0"


def test_the_last_committed_batch_names_its_query_and_source_offsets() -> None:
    files = FakeCheckpoints(
        checkpoint(
            offsets(snapshot_offset(10)),
            offsets(snapshot_offset(20)),
            offsets(snapshot_offset(30)),
            committed=1,
        )
    )

    batch = committed_batch(files, CHECKPOINT)

    assert batch is not None
    assert (batch.query_id, batch.batch) == (QUERY_ID, 1)
    assert batch.sources == ({"version": 1, "snapshot_id": 20, "position": 3},)
    assert committed_snapshots(files, CHECKPOINT) == (20,)


def test_a_stream_that_never_committed_has_no_batch_and_no_snapshot() -> None:
    files = FakeCheckpoints(checkpoint(offsets(snapshot_offset(10)), committed=None))

    assert committed_batch(files, CHECKPOINT) is None
    assert committed_snapshots(files, CHECKPOINT) is None


def test_a_file_source_has_read_the_files_of_its_log_up_to_the_committed_offset() -> None:
    files = FakeCheckpoints(
        checkpoint(offsets({"logOffset": 0}), offsets({"logOffset": 2}), committed=1)
        | {
            f"{SOURCES}/0": file_log(("a.avro", 0)),
            f"{SOURCES}/1": file_log(("b.avro", 1)),
            f"{SOURCES}/2": file_log(("c.avro", 2), ("d.avro", 2)),
            f"{SOURCES}/3": file_log(("e.avro", 3)),
        }
    )
    batch = committed_batch(files, CHECKPOINT)
    assert batch is not None

    assert committed_files(files, CHECKPOINT, batch) == {"a.avro", "b.avro", "c.avro", "d.avro"}


def test_a_compacted_log_repeats_earlier_entries_and_may_go_past_the_committed_offset() -> None:
    files = FakeCheckpoints(
        checkpoint(offsets({"logOffset": 9}), offsets({"logOffset": 11}), committed=1)
        | {
            f"{SOURCES}/8": file_log(("i.avro", 8)),
            f"{SOURCES}/9.compact": file_log(("a.avro", 0), ("i.avro", 8), ("j.avro", 9)),
            f"{SOURCES}/10": file_log(("k.avro", 10)),
            f"{SOURCES}/11": file_log(("l.avro", 11)),
            f"{SOURCES}/19.compact": file_log(("a.avro", 0), ("m.avro", 12)),
        }
    )
    batch = committed_batch(files, CHECKPOINT)
    assert batch is not None

    assert committed_files(files, CHECKPOINT, batch) == {
        "a.avro",
        "i.avro",
        "j.avro",
        "k.avro",
        "l.avro",
    }


def test_hadoop_files_list_and_read_a_checkpoint(spark: SparkSession, tmp_path: Path) -> None:
    root = f"file://{tmp_path}"
    for path, text in checkpoint(offsets(snapshot_offset(40)), committed=0, root=root).items():
        local = Path(path.removeprefix("file://"))
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(text)
    files = HadoopCheckpointFiles(spark)

    assert files.names(f"{root}/missing") is None
    assert committed_snapshots(files, root) == (40,)


def test_the_committed_snapshots_need_only_the_commits_and_the_offsets() -> None:
    files = FakeCheckpoints(
        {
            f"{CHECKPOINT}/commits/0": "",
            f"{CHECKPOINT}/offsets/0": offsets(snapshot_offset(10), snapshot_offset(11)),
            f"{CHECKPOINT}/offsets/1": offsets(snapshot_offset(20)),
        }
    )

    assert committed_snapshots(files, CHECKPOINT) == (10, 11)
