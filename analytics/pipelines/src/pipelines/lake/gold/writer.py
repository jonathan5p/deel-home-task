import threading
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.batch import Reader, Write
from pipelines.lake.gold.model.columns import IS_REMOVED
from pipelines.lake.gold.model.versions import all_of
from pipelines.lake.gold.serving import COPIES, Change, Copy
from pipelines.lake.gold.stored import StoredTable, WriteMode

MAX_RETRIES = 3
COMMIT_FAILED = "CommitFailedException"
CONFLICTING_FILES = ("ValidationException", "conflicting")


class Batches(Protocol):
    """The writes of each micro-batch, as `GoldBatches` computes them."""

    @property
    def tables(self) -> Mapping[str, StoredTable]: ...

    def writes(self, load: str, batch: DataFrame, read: Reader) -> list[Write]: ...


class Lake(Protocol):
    """The gold tables in Iceberg, as the writer uses them."""

    def read(self, name: str) -> DataFrame: ...

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None: ...

    def snapshot(self, name: str) -> int | None: ...

    def unchanged(self, table: StoredTable, rows: DataFrame) -> bool: ...

    def write(self, table: StoredTable, rows: DataFrame) -> None: ...


class Serving(Protocol):
    """The serving copy of the gold tables, as the writer uses it."""

    def synced(self) -> Mapping[str, int | None]: ...

    def apply(self, changes: Sequence[Change], snapshots: Mapping[str, int | None]) -> None: ...


def is_commit_conflict(error: BaseException) -> bool:
    """Whether an Iceberg commit failed because another commit changed the table first, which
    replaying the batch resolves."""
    message = str(error)
    return COMMIT_FAILED in message or all(part in message for part in CONFLICTING_FILES)


def materialized(rows: DataFrame) -> DataFrame:
    """The rows computed now, so writing one table never changes what another write reads."""
    return rows.localCheckpoint(eager=True)


def difference(table: StoredTable, before: DataFrame, after: DataFrame) -> Change:
    """The change that turns a copy of `before` into `after`: the rows added or updated, and
    for a recounted table the keys no longer there."""
    if table.mode is WriteMode.REPLACE:
        return Change(table, after, Copy.REPLACE)
    columns = list(table.columns)
    added = after.select(*columns).exceptAll(before.select(*columns))
    if table.mode is not WriteMode.RECOUNT:
        return Change(table, added, Copy.UPSERT)
    keys = table.keys
    current = after.select(*(F.col(key).alias(f"__after_{key}") for key in keys))
    matching = all_of([F.col(key).eqNullSafe(F.col(f"__after_{key}")) for key in keys])
    gone = before.join(current, matching, "left_anti").select(*columns)
    return Change(
        table,
        added.withColumn(IS_REMOVED, F.lit(False)).unionByName(
            gone.withColumn(IS_REMOVED, F.lit(True))
        ),
        Copy.RECOUNT,
    )


class GoldWriter:
    """Writes each micro-batch of every gold load to Iceberg, then copies what it wrote into
    the serving copy, one batch at a time.

    One lock is held for a whole batch, from reading the stored tables to the copy's commit, so
    a fact batch never resolves against dimension versions another batch is changing and no two
    batches merge into the same table at once. A batch whose Iceberg commit conflicts with
    another commit is computed and written again, up to `max_retries` times: every write
    depends only on the stored rows and the batch, so a replay is idempotent.

    The copy holds the Iceberg snapshot of each table it copied. A table written to Iceberg but
    not copied yet, because its batch failed, is copied before anything else: from the
    difference between the two snapshots, or whole when the copy has no snapshot of the table
    or its snapshot is gone.
    """

    def __init__(
        self,
        batches: Batches,
        lake: Lake,
        serving: Serving | None,
        *,
        lock: AbstractContextManager[object] | None = None,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.batches = batches
        self.lake = lake
        self.serving = serving
        self.lock = lock or threading.Lock()
        self.max_retries = max_retries
        self.unsynced: set[str] = set()

    @property
    def tables(self) -> Mapping[str, StoredTable]:
        return self.batches.tables

    def write_seeds(self, seeds: Mapping[str, DataFrame]) -> None:
        """Merge the rows of the seeded tables that changed, then copy them."""
        writes = [Write(self.tables[name], rows) for name, rows in seeds.items()]
        with self.lock:
            self._apply([Write(w.table, self._seed_changes(w)) for w in writes])

    def _seed_changes(self, write: Write) -> DataFrame:
        stored = self.lake.read(write.table.name).select(*write.table.columns)
        return materialized(write.rows.select(*write.table.columns).exceptAll(stored))

    def sync(self, names: Sequence[str] | None = None, *, reload: bool = False) -> list[str]:
        """Copy every table whose Iceberg snapshot the copy does not hold yet; with `reload`,
        copy each whole. Returns the tables copied."""
        with self.lock:
            return self._sync(list(names or self.tables), reload=reload)

    def _sync(self, names: Sequence[str], *, reload: bool) -> list[str]:
        if self.serving is None or not names:
            return []
        synced = self.serving.synced()
        changes: list[Change] = []
        snapshots: dict[str, int | None] = {}
        for name in names:
            current = self.lake.snapshot(name)
            if not reload and name in synced and synced[name] == current:
                continue
            changes.append(self._catch_up(self.tables[name], synced, reload=reload))
            snapshots[name] = current
        self.serving.apply(changes, snapshots)
        self.unsynced -= set(snapshots)
        return list(snapshots)

    def _catch_up(
        self, table: StoredTable, synced: Mapping[str, int | None], *, reload: bool
    ) -> Change:
        current = self.lake.read(table.name)
        if reload or table.name not in synced:
            return Change(table, current, Copy.RELOAD)
        snapshot = synced[table.name]
        if snapshot is None:
            return difference(table, current.limit(0), current)
        before = self.lake.read_snapshot(table.name, snapshot)
        if before is None:
            return Change(table, current, Copy.RELOAD)
        return difference(table, before, current)

    def write_batch(self, load: str, batch: DataFrame) -> None:
        """Write one micro-batch of `load`, and copy what it wrote, under the gold lock."""
        with self.lock:
            attempt = 0
            while True:
                try:
                    self._sync(sorted(self.unsynced), reload=False)
                    self._apply(self.batches.writes(load, batch, self.lake.read))
                    return
                except Exception as error:
                    if attempt >= self.max_retries or not is_commit_conflict(error):
                        raise
                    attempt += 1

    def _apply(self, writes: Sequence[Write]) -> None:
        computed = [Write(write.table, materialized(write.rows)) for write in writes]
        written: list[Write] = []
        for write in computed:
            if self.lake.unchanged(write.table, write.rows):
                continue
            self.unsynced.add(write.table.name)
            self.lake.write(write.table, write.rows)
            written.append(write)
        if self.serving is None or not written:
            return
        snapshots = {write.table.name: self.lake.snapshot(write.table.name) for write in written}
        changes = [Change(w.table, w.rows, COPIES[w.table.mode]) for w in written]
        self.serving.apply(changes, snapshots)
        self.unsynced -= set(snapshots)
