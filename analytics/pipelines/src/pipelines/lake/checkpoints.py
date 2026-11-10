import json
from dataclasses import dataclass
from typing import Any, Protocol

from pyspark.sql import SparkSession


class CheckpointFiles(Protocol):
    def names(self, folder: str) -> list[str] | None:
        """The entries of `folder`, or None when it does not exist."""
        ...

    def text(self, path: str) -> str: ...


class HadoopCheckpointFiles:
    """Reads checkpoints through Spark's Hadoop file systems, so s3a:// and file:// both work."""

    def __init__(self, spark: SparkSession) -> None:
        jvm = spark.sparkContext._jvm
        java_context = spark.sparkContext._jsc
        assert jvm is not None
        assert java_context is not None
        self._jvm = jvm
        self._conf = java_context.hadoopConfiguration()

    def _file_system(self, path: str) -> tuple[Any, Any]:
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        return hadoop_path.getFileSystem(self._conf), hadoop_path

    def names(self, folder: str) -> list[str] | None:
        file_system, path = self._file_system(folder)
        if not file_system.exists(path):
            return None
        return [status.getPath().getName() for status in file_system.listStatus(path)]

    def text(self, path: str) -> str:
        file_system, hadoop_path = self._file_system(path)
        stream = file_system.open(hadoop_path)
        try:
            return str(self._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
        finally:
            stream.close()


type Offset = dict[str, Any]


@dataclass(frozen=True, slots=True)
class CommittedBatch:
    """A stream's last committed batch: the query that ran it, its number, and the offset each
    source had read up to, in source order."""

    query_id: str
    batch: int
    sources: tuple[Offset, ...]


def _batches(files: CheckpointFiles, folder: str) -> list[int]:
    return [int(name) for name in files.names(folder) or () if name.isdecimal()]


def _sources(files: CheckpointFiles, checkpoint: str, batch: int) -> tuple[Offset, ...]:
    lines = files.text(f"{checkpoint}/offsets/{batch}").splitlines()[2:]
    return tuple(json.loads(line) for line in lines if line.startswith("{"))


def _batch(files: CheckpointFiles, checkpoint: str, batch: int) -> CommittedBatch:
    query_id = json.loads(files.text(f"{checkpoint}/metadata"))["id"]
    return CommittedBatch(
        query_id=query_id, batch=batch, sources=_sources(files, checkpoint, batch)
    )


def committed_batch(files: CheckpointFiles, checkpoint: str) -> CommittedBatch | None:
    """The last batch the stream committed, or None before its first commit.

    A batch is committed once `commits/<batch>` exists; its `offsets/<batch>` file holds a version
    line, a metadata line, then one offset per source. `metadata` names the query.
    """
    batches = _batches(files, f"{checkpoint}/commits")
    return _batch(files, checkpoint, max(batches)) if batches else None


def planned_batch(files: CheckpointFiles, checkpoint: str) -> CommittedBatch | None:
    """The batch the stream planned after its last commit, or None when it has none.

    A batch's offsets are written before it runs, so its sink may already hold its output while
    `commits/` does not list it yet.
    """
    committed = max(_batches(files, f"{checkpoint}/commits"), default=-1)
    planned = max(_batches(files, f"{checkpoint}/offsets"), default=-1)
    return _batch(files, checkpoint, planned) if planned > committed else None


def committed_snapshots(files: CheckpointFiles, checkpoint: str) -> tuple[int, ...] | None:
    """The Iceberg snapshots a stream had read up to in its last committed batch, or None before
    its first commit; an Iceberg source's offset names a `snapshot_id`."""
    batches = _batches(files, f"{checkpoint}/commits")
    if not batches:
        return None
    return tuple(source["snapshot_id"] for source in _sources(files, checkpoint, max(batches)))


def committed_files(
    files: CheckpointFiles, checkpoint: str, committed: CommittedBatch, source: int = 0
) -> frozenset[str]:
    """The files a file-stream source had read up to the committed batch.

    The source's offset is a `logOffset`, the last entry of its log under `sources/<source>/`. Each
    log file, numbered or a `.compact` that repeats the earlier entries, has a version line, then
    one JSON line per file with the log entry it was read in.
    """
    log_offset = int(committed.sources[source]["logOffset"])
    folder = f"{checkpoint}/sources/{source}"
    paths: set[str] = set()
    for name in files.names(folder) or ():
        number = name.removesuffix(".compact")
        if not number.isdecimal() or int(number) > log_offset:
            continue
        for line in files.text(f"{folder}/{name}").splitlines()[1:]:
            entry = json.loads(line)
            if int(entry["batchId"]) <= log_offset:
                paths.add(entry["path"])
    return frozenset(paths)
