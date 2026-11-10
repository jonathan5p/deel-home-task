from pathlib import Path

from pyspark.sql import SparkSession

from analytics.pipelines.tests.maintenance.job_setup import FakeCheckpoints, offsets
from pipelines.maintenance.checkpoints import HadoopCheckpointFiles, committed_snapshots

CHECKPOINT = "s3a://lake/checkpoints/silver/orders"


def test_the_last_committed_batch_gives_the_snapshot_a_restart_resumes_from() -> None:
    files = FakeCheckpoints(
        {
            f"{CHECKPOINT}/commits/0": "",
            f"{CHECKPOINT}/commits/1": "",
            f"{CHECKPOINT}/offsets/0": offsets(10),
            f"{CHECKPOINT}/offsets/1": offsets(20),
            f"{CHECKPOINT}/offsets/2": offsets(30),
        }
    )

    assert committed_snapshots(files, CHECKPOINT) == (20,)


def test_a_stream_that_never_committed_has_no_snapshot() -> None:
    files = FakeCheckpoints({f"{CHECKPOINT}/offsets/0": offsets(10)})

    assert committed_snapshots(files, CHECKPOINT) is None


def test_hadoop_files_list_and_read_a_checkpoint(spark: SparkSession, tmp_path: Path) -> None:
    (tmp_path / "commits").mkdir()
    (tmp_path / "commits" / "4").write_text("v1")
    (tmp_path / "offsets").mkdir()
    (tmp_path / "offsets" / "4").write_text(offsets(40))
    files = HadoopCheckpointFiles(spark)

    assert files.names(f"file://{tmp_path}/missing") is None
    assert committed_snapshots(files, f"file://{tmp_path}") == (40,)
