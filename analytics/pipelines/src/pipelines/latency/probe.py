import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from pipelines.lake.bronze.entities import Entity, entity_for_topic
from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.settings import ProbeOptions
from pipelines.latency.sql import Row, integer, literal, relation

MARKER_COLUMN = "updated_by"
DOWNSTREAM = LAYERS[1:]


class ProbeSetupError(RuntimeError):
    """The probe cannot run: an entity, row or table it needs is missing."""


@dataclass(frozen=True, slots=True)
class SourceTable:
    """The source table of an entity, named by the last two segments of its CDC topic
    (`<prefix>.<schema>.<table>`)."""

    entity: Entity
    topic: str

    def __post_init__(self) -> None:
        if MARKER_COLUMN not in self.entity.audit_columns:
            raise ProbeSetupError(
                f"{self.entity.name} has no audit column {MARKER_COLUMN} to mark probes with"
            )
        if len(self.topic.split(".")) < 3:
            raise ProbeSetupError(f"topic {self.topic!r} does not name <prefix>.<schema>.<table>")

    @property
    def name(self) -> str:
        return self.entity.name

    @property
    def relation(self) -> str:
        schema, table = self.topic.split(".")[-2:]
        return relation(schema, table)

    @property
    def key_column(self) -> str:
        return self.entity.key


@dataclass(frozen=True, slots=True)
class ProbeTarget:
    """The source row of one entity that every probe marks."""

    table: SourceTable
    key: int

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def entity(self) -> Entity:
        return self.table.entity


def source_tables(entities: Sequence[str], topics: Sequence[str]) -> list[SourceTable]:
    """The source table of each named entity, from the topic that carries its changes."""
    by_name: dict[str, str] = {}
    for topic in topics:
        by_name[entity_for_topic(topic).name] = topic
    missing = [name for name in entities if name not in by_name]
    if missing:
        raise ProbeSetupError(f"KAFKA_TOPICS has no topic for {', '.join(missing)}")
    return [SourceTable(entity_for_topic(by_name[name]), by_name[name]) for name in entities]


def lowest_keys_query(tables: Sequence[SourceTable]) -> str:
    return " UNION ALL ".join(
        f"SELECT {literal(table.name)} AS entity,"
        f" min({relation(table.key_column)}) AS key FROM {table.relation}"
        for table in tables
    )


def marker_statement(targets: Sequence[ProbeTarget], marker: int) -> str:
    """One statement, so one source transaction, that sets the marker column of every target
    row and returns how many rows of each entity it updated.

    The marker column is an audit column, which the content hash leaves out, so the change
    reaches every layer without changing any business value.
    """
    updates = ", ".join(
        f"probe_{index} AS (UPDATE {target.table.relation}"
        f" SET {relation(MARKER_COLUMN)} = {marker}"
        f" WHERE {relation(target.table.key_column)} = {target.key} RETURNING 1)"
        for index, target in enumerate(targets)
    )
    counts = " UNION ALL ".join(
        f"SELECT {literal(target.name)} AS entity, count(*) AS updated FROM probe_{index}"
        for index, target in enumerate(targets)
    )
    return f"WITH {updates} {counts}"


def counts_by_entity(rows: Sequence[Row]) -> dict[str, int | None]:
    return {str(name): None if value is None else integer(value) for name, value in rows}


class Source(Protocol):
    def lowest_keys(self, tables: Sequence[SourceTable]) -> Mapping[str, int | None]: ...

    def mark(self, targets: Sequence[ProbeTarget], marker: int) -> Mapping[str, int | None]: ...


@dataclass(frozen=True, slots=True)
class QuerySource:
    """The source database, through a function that runs one SQL statement on it."""

    query: Callable[[str], Sequence[Row]]

    def lowest_keys(self, tables: Sequence[SourceTable]) -> dict[str, int | None]:
        return counts_by_entity(self.query(lowest_keys_query(tables)))

    def mark(self, targets: Sequence[ProbeTarget], marker: int) -> dict[str, int | None]:
        return counts_by_entity(self.query(marker_statement(targets, marker)))


def probe_targets(
    tables: Sequence[SourceTable], keys: Mapping[str, int], source: Source
) -> list[ProbeTarget]:
    """The row each entity's probes mark: the configured key, otherwise the lowest one."""
    lowest = source.lowest_keys([table for table in tables if table.name not in keys])
    targets = []
    for table in tables:
        key = keys.get(table.name, lowest.get(table.name))
        if key is None:
            raise ProbeSetupError(f"{table.relation} has no rows to probe")
        targets.append(ProbeTarget(table, key))
    return targets


@dataclass(frozen=True, slots=True)
class Change:
    """A probe's change as the landing zone recorded it; times are epoch milliseconds."""

    commit_sequence: int
    changed_at: int
    captured_at: int
    published_at: int


class LandingSearch(Protocol):
    def mark(self) -> None:
        """Remember what has landed so far, so `find` only searches what lands after."""

    def find(self, target: ProbeTarget, marker: int) -> Change | None: ...


class LayerLookup(Protocol):
    def visible(self, target: ProbeTarget, change: Change) -> bool: ...


@dataclass(frozen=True, slots=True)
class Observation:
    """When each layer was first seen holding one entity's change, in epoch milliseconds."""

    entity: str
    key: int
    change: Change | None
    observed_at: Mapping[Layer, int]


@dataclass(frozen=True, slots=True)
class ProbeRound:
    number: int
    marker: int
    waited_seconds: float
    committed_at: int
    observations: tuple[Observation, ...]


@dataclass(frozen=True, slots=True)
class Timing:
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    random: Callable[[], float] = random.random

    def now(self) -> int:
        return round(self.clock() * 1000)


@dataclass(slots=True)
class _Progress:
    target: ProbeTarget
    change: Change | None = None
    observed_at: dict[Layer, int] = field(default_factory=dict)

    @property
    def done(self) -> bool:
        return len(self.observed_at) == len(LAYERS)

    def observation(self) -> Observation:
        return Observation(self.target.name, self.target.key, self.change, dict(self.observed_at))


@dataclass(frozen=True, slots=True)
class Probe:
    source: Source
    landing: Mapping[str, LandingSearch]
    lookups: Mapping[Layer, LayerLookup]
    options: ProbeOptions
    max_wait_seconds: float
    timing: Timing = Timing()

    def run(
        self,
        targets: Sequence[ProbeTarget],
        after_round: Callable[[ProbeRound], None] = lambda _: None,
    ) -> list[ProbeRound]:
        rounds: list[ProbeRound] = []
        for number in range(1, self.options.probes + 1):
            last_marker = rounds[-1].marker if rounds else 0
            rounds.append(self.run_round(number, targets, last_marker))
            after_round(rounds[-1])
        return rounds

    def run_round(
        self, number: int, targets: Sequence[ProbeTarget], last_marker: int = 0
    ) -> ProbeRound:
        """Wait a random part of the longest trigger interval, so probes land at different
        points of the jobs' cycles, then mark every target in one transaction and poll each
        layer until all have the change or the timeout passes."""
        waited = self.timing.random() * self.max_wait_seconds
        self.timing.sleep(waited)
        for search in self.landing.values():
            search.mark()
        marker = min(-self.timing.now(), last_marker - 1)
        updated = self.source.mark(targets, marker)
        committed_at = self.timing.now()
        unmarked = [target for target in targets if updated.get(target.name) != 1]
        if unmarked:
            rows = ", ".join(f"{t.table.relation} {t.table.key_column} = {t.key}" for t in unmarked)
            raise ProbeSetupError(f"the probe found no source row to mark: {rows}")
        progress = [_Progress(target) for target in targets]
        self._poll(progress, marker, committed_at)
        return ProbeRound(
            number=number,
            marker=marker,
            waited_seconds=waited,
            committed_at=committed_at,
            observations=tuple(item.observation() for item in progress),
        )

    def _poll(self, progress: Sequence[_Progress], marker: int, committed_at: int) -> None:
        timeout = self.options.timeout_seconds * 1000
        while True:
            polled_at = self.timing.now()
            for item in progress:
                self._check(item, marker, polled_at)
            if all(item.done for item in progress) or polled_at - committed_at >= timeout:
                return
            elapsed = self.timing.clock() - polled_at / 1000
            self.timing.sleep(max(0.0, self.options.poll_seconds - elapsed))

    def _check(self, item: _Progress, marker: int, polled_at: int) -> None:
        """Downstream layers are checked last to first: a layer only gets a change after the
        one before it, so a poll never sees a later layer before an earlier one."""
        if item.change is None:
            item.change = self.landing[item.target.name].find(item.target, marker)
            if item.change is None:
                return
            item.observed_at[Layer.LANDING] = polled_at
        for layer in reversed(DOWNSTREAM):
            if layer not in item.observed_at and self.lookups[layer].visible(
                item.target, item.change
            ):
                item.observed_at[layer] = polled_at
