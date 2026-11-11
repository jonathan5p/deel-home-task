import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from pipelines.latency.probe import MARKER_COLUMN, Change, ProbeTarget
from pipelines.latency.sql import Row, duckdb_path, integer, literal

SINK_LOG_VERSION = "v1"
COMPACT_SUFFIX = ".compact"
ADD = "add"
DELETE = "delete"


class SinkLogError(ValueError):
    def __init__(self, name: str, problem: str) -> None:
        super().__init__(f"file sink log {name}: {problem}")


@dataclass(frozen=True, slots=True)
class LogEntry:
    path: str
    action: str


def parse_sink_log(name: str, text: str) -> list[LogEntry]:
    """The entries of one file sink log (`_spark_metadata/<batch>[.compact]`): a `v1` header
    line, then one JSON object per file."""
    header, *lines = text.splitlines()
    if header.strip() != SINK_LOG_VERSION:
        raise SinkLogError(name, f"unsupported version {header.strip()!r}")
    entries = []
    for line in lines:
        if not line.strip():
            continue
        entry = json.loads(line)
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise SinkLogError(name, f"not a file entry: {line}")
        entries.append(LogEntry(path=entry["path"], action=str(entry.get("action", ADD))))
    return entries


def batch_id(name: str) -> int | None:
    """The batch a log file belongs to (`7` and `9.compact`), or None for any other file."""
    number = name.removesuffix(COMPACT_SUFFIX)
    return int(number) if number.isdecimal() else None


def current_logs(names: Iterable[str]) -> list[str]:
    """The logs that together list every file the sink has committed: the latest compact log,
    which holds every batch up to its own, and the batch logs after it, in batch order."""
    batches = sorted((batch, name) for name in names if (batch := batch_id(name)) is not None)
    compacts = [batch for batch, name in batches if name.endswith(COMPACT_SUFFIX)]
    start = compacts[-1] if compacts else -1
    return [
        name
        for batch, name in batches
        if batch > start or (batch == start and name.endswith(COMPACT_SUFFIX))
    ]


def committed_files(logs: Iterable[Sequence[LogEntry]]) -> set[str]:
    files: set[str] = set()
    for entries in logs:
        for entry in entries:
            if entry.action == DELETE:
                files.discard(entry.path)
            else:
                files.add(entry.path)
    return files


class SinkLogStore(Protocol):
    def log_names(self) -> list[str]: ...

    def read_log(self, name: str) -> str: ...


@dataclass(slots=True)
class SinkLog:
    """The files a Structured Streaming file sink has committed, read from its log; each log
    file is read once, since the sink never rewrites one."""

    store: SinkLogStore
    _logs: dict[str, list[LogEntry]] = field(default_factory=dict)
    _seen: set[str] = field(default_factory=set)

    def files(self) -> set[str]:
        names = current_logs(self.store.log_names())
        for name in names:
            if name not in self._logs:
                self._logs[name] = parse_sink_log(name, self.store.read_log(name))
        return committed_files(self._logs[name] for name in names)

    def mark(self) -> None:
        self._seen = self.files()

    def new_files(self) -> list[str]:
        """The files committed since the last `mark` or `new_files`, in path order."""
        files = self.files()
        new = sorted(files - self._seen)
        self._seen |= files
        return new


def landing_match_query(records: str, target: ProbeTarget, marker: int) -> str:
    """The change of `target` that carries `marker`, from `records`, a relation with the
    landed Kafka columns (`value` and `timestamp`)."""
    after = f"$.after.{target.table.key_column}"
    return (
        "SELECT json_extract(change, '$.source.lsn')::BIGINT,"
        " json_extract(change, '$.source.ts_ms')::BIGINT,"
        " json_extract(change, '$.ts_ms')::BIGINT,"
        " epoch_ms(published)"
        f' FROM (SELECT decode(value) AS change, "timestamp" AS published FROM {records})'
        f" WHERE json_extract(change, {literal(after)})::BIGINT = {target.key}"
        f" AND json_extract(change, {literal('$.after.' + MARKER_COLUMN)})::BIGINT = {marker}"
        " ORDER BY 1 LIMIT 1"
    )


def change_of(rows: Sequence[Row]) -> Change | None:
    if not rows:
        return None
    lsn, changed_at, captured_at, published_at = rows[0]
    return Change(
        commit_sequence=integer(lsn),
        changed_at=integer(changed_at),
        captured_at=integer(captured_at),
        published_at=integer(published_at),
    )


def avro_files(files: Sequence[str]) -> str:
    return f"read_avro([{', '.join(literal(duckdb_path(file)) for file in files)}])"


@dataclass(slots=True)
class LandingWatch:
    """Finds a probe's change among the files one topic's landing sink commits after `mark`."""

    log: SinkLog
    query: Callable[[str], Sequence[Row]]
    records: Callable[[Sequence[str]], str] = avro_files

    def mark(self) -> None:
        self.log.mark()

    def find(self, target: ProbeTarget, marker: int) -> Change | None:
        files = self.log.new_files()
        if not files:
            return None
        return change_of(self.query(landing_match_query(self.records(files), target, marker)))
