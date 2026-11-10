import json
from typing import Any, Protocol

from pyspark.sql import SparkSession


class CheckpointFiles(Protocol):
    def names(self, folder: str) -> list[str] | None:
        """The entries of `folder`, or None when it does not exist."""
        ...

    def text(self, path: str) -> str: ...


class HadoopCheckpointFiles:
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


def committed_snapshots(files: CheckpointFiles, checkpoint: str) -> tuple[int, ...] | None:
    """The Iceberg snapshots a stream had read up to in its last committed batch, or None before
    its first commit."""
    batches = [int(name) for name in files.names(f"{checkpoint}/commits") or () if name.isdecimal()]
    if not batches:
        return None
    offsets = files.text(f"{checkpoint}/offsets/{max(batches)}").splitlines()[2:]
    return tuple(json.loads(line)["snapshot_id"] for line in offsets if line.startswith("{"))
