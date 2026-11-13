import time
from collections.abc import Mapping, Sequence
from typing import Protocol

from pyspark.sql import DataFrame

from pipelines.lake.gold.batch import Reader, Write
from pipelines.lake.gold.metrics import BatchRun, Clock, JobIds, no_jobs
from pipelines.lake.gold.serving import Change, Copy
from pipelines.lake.gold.stored import StoredTable

MAX_RETRIES = 3
COMMIT_FAILED = "CommitFailedException"
CONFLICTING_FILES = ("ValidationException", "conflicting")


class Batches(Protocol):
    """The writes of each micro-batch, as `GoldBatches` computes them."""

    @property
    def tables(self) -> Mapping[str, StoredTable]: ...

    def tables_of(self, load: str) -> tuple[str, ...]: ...

    def staged(self, load: str, batch: DataFrame) -> DataFrame: ...

    def writes(self, load: str, staged: DataFrame, read: Reader) -> list[Write]: ...


class Lake(Protocol):
    """The gold tables in Iceberg, as the writer uses them."""

    def read(self, name: str) -> DataFrame: ...

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None: ...

    def snapshot(self, name: str) -> int | None: ...

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


def computed(rows: DataFrame) -> tuple[DataFrame, int]:
    """The rows computed once, and how many there are, in one Spark job. Their lineage is cut,
    so the merge and the copy read the same rows even after the merge changes the stored
    table they were computed from."""
    rows = rows.localCheckpoint(eager=False)
    return rows, rows.count()


def difference(table: StoredTable, before: DataFrame, after: DataFrame) -> Change:
    """The change that turns a copy of `before` into `after`: the rows added or updated. No
    write removes a row, so none is ever missing from `after`."""
    columns = list(table.columns)
    return Change(table, after.select(*columns).exceptAll(before.select(*columns)))


class GoldWriter:
    """Writes each micro-batch of a gold load to its Iceberg tables, one `mergeInto` per table,
    then copies the rows it merged into the serving copy, in one transaction with the
    snapshots they came from.

    A load writes only its own tables, so the loads' streams run at the same time without a
    lock. A batch whose Iceberg commit conflicts with another commit, such as the lake's
    maintenance, is computed and written again, up to `max_retries` times: every write depends
    only on the stored rows and the batch, so a replay is idempotent.

    The copy holds the Iceberg snapshot of each table it copied. A table written to Iceberg but
    not copied yet, because its batch failed, is caught up before the load's next batch, and
    every table when the job starts: from the difference between the two snapshots, or whole
    when the copy has no snapshot of the table or its snapshot is gone.
    """

    def __init__(
        self,
        batches: Batches,
        lake: Lake,
        serving: Serving | None,
        *,
        max_retries: int = MAX_RETRIES,
        jobs: JobIds = no_jobs,
        clock: Clock = time.perf_counter,
    ) -> None:
        self.batches = batches
        self.lake = lake
        self.serving = serving
        self.max_retries = max_retries
        self.jobs = jobs
        self.clock = clock
        self.unsynced: set[str] = set()

    @property
    def tables(self) -> Mapping[str, StoredTable]:
        return self.batches.tables

    def write_seeds(self, seeds: Mapping[str, DataFrame]) -> None:
        """Merge the rows of the seeded tables that changed, then copy them."""
        writes = []
        for name, rows in seeds.items():
            table = self.tables[name]
            stored = self.lake.read(name).select(*table.columns)
            writes.append(Write(table, rows.select(*table.columns).exceptAll(stored)))
        self._apply(writes, BatchRun("seeds", None, self.jobs, self.clock))

    def sync(self, names: Sequence[str] | None = None, *, reload: bool = False) -> list[str]:
        """Copy every table whose Iceberg snapshot the copy does not hold yet; with `reload`,
        copy each whole. Returns the tables copied."""
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
            return Change(table, current)
        before = self.lake.read_snapshot(table.name, snapshot)
        if before is None:
            return Change(table, current, Copy.RELOAD)
        return difference(table, before, current)

    def write_batch(self, load: str, batch: DataFrame, batch_id: int | None = None) -> None:
        """Write one micro-batch of `load` and copy what it wrote; logs one line with the time
        and Spark jobs of each step."""
        run = BatchRun(load, batch_id, self.jobs, self.clock)
        staged = self.batches.staged(load, batch).persist()
        try:
            attempt = 0
            while True:
                try:
                    with run.step("catch-up"):
                        own = self.batches.tables_of(load)
                        self._sync([name for name in own if name in self.unsynced], reload=False)
                    self._apply(self.batches.writes(load, staged, self.lake.read), run)
                    break
                except Exception as error:
                    if attempt >= self.max_retries or not is_commit_conflict(error):
                        raise
                    attempt += 1
        finally:
            staged.unpersist()
        run.log()

    def _apply(self, writes: Sequence[Write], run: BatchRun) -> None:
        written: list[Write] = []
        for write in writes:
            name = write.table.name
            with run.step(f"compute {name}"):
                rows, count = computed(write.rows)
            run.written[name] = count
            if count == 0:
                continue
            self.unsynced.add(name)
            with run.step(f"merge {name}"):
                self.lake.write(write.table, rows)
            written.append(Write(write.table, rows))
        if self.serving is None or not written:
            return
        with run.step("snapshots"):
            snapshots = {w.table.name: self.lake.snapshot(w.table.name) for w in written}
        with run.step("copy"):
            self.serving.apply([Change(w.table, w.rows) for w in written], snapshots)
        self.unsynced -= set(snapshots)
