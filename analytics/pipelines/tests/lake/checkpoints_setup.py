import json
from collections.abc import Mapping

CHECKPOINT = "s3a://lake/checkpoints/bronze/orders"
QUERY_ID = "6f4f2a1e-query"


class FakeCheckpoints:
    """Checkpoint files held in memory, keyed by path."""

    def __init__(self, files: Mapping[str, str] | None = None) -> None:
        self.files = dict(files or {})

    def names(self, folder: str) -> list[str] | None:
        prefix = f"{folder}/"
        names = {
            path.removeprefix(prefix).split("/")[0]
            for path in self.files
            if path.startswith(prefix)
        }
        return sorted(names) or None

    def text(self, path: str) -> str:
        return self.files[path]


def offsets(*sources: Mapping[str, object]) -> str:
    """An offsets file as Spark writes it: version, batch metadata, then one source per line."""
    return "\n".join(
        ["v1", '{"batchWatermarkMs":0,"batchTimestampMs":0}', *map(json.dumps, sources)]
    )


def snapshot_offset(snapshot_id: int) -> dict[str, object]:
    """An Iceberg source's offset."""
    return {"version": 1, "snapshot_id": snapshot_id, "position": 3}


def file_log(*entries: tuple[str, int]) -> str:
    """A file-stream source's log file: a version line, then one entry per file and log batch."""
    lines = [
        json.dumps({"path": path, "timestamp": 0, "batchId": batch}) for path, batch in entries
    ]
    return "\n".join(["v1", *lines])


def checkpoint(
    *batches: str, committed: int | None, query_id: str = QUERY_ID, root: str = CHECKPOINT
) -> dict[str, str]:
    """A checkpoint whose batches have the given offsets files, committed up to `committed`."""
    files = {f"{root}/metadata": json.dumps({"id": query_id})}
    for batch, offsets_file in enumerate(batches):
        files[f"{root}/offsets/{batch}"] = offsets_file
    for batch in range(0 if committed is None else committed + 1):
        files[f"{root}/commits/{batch}"] = "v1\n{}"
    return files
