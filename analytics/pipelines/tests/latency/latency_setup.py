from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from pipelines.bronze.entities import CUSTOMERS, ORDERS
from pipelines.latency.layers import Layer
from pipelines.latency.probe import (
    Change,
    Observation,
    ProbeRound,
    ProbeTarget,
    SourceTable,
    Timing,
)

CUSTOMERS_TOPIC = "prefix.operations.customers"
ORDERS_TOPIC = "prefix.operations.orders"
TOPICS = (CUSTOMERS_TOPIC, ORDERS_TOPIC)
CUSTOMERS_TABLE = SourceTable(CUSTOMERS, CUSTOMERS_TOPIC)
ORDERS_TABLE = SourceTable(ORDERS, ORDERS_TOPIC)
CUSTOMER = ProbeTarget(CUSTOMERS_TABLE, 7)
ORDER = ProbeTarget(ORDERS_TABLE, 3)

COMMITTED_AT = 1_000_000
CHANGE = Change(
    commit_sequence=500,
    changed_at=COMMITTED_AT,
    captured_at=COMMITTED_AT + 100,
    published_at=COMMITTED_AT + 300,
)

ENV = {
    "KAFKA_TOPICS": ",".join(TOPICS),
    "LAKE_ROOT": "s3a://bucket/",
    "ICEBERG_CATALOG": "catalog",
    "LAKE_CATALOG_URI": "http://catalog:8181/",
    "LAKE_S3_ENDPOINT": "http://storage:8333",
    "LAKE_S3_REGION": "region-1",
    "LAKE_S3_ACCESS_KEY": "access",
    "LAKE_S3_SECRET_KEY": "secret",
    "SOURCE_DB_URL": "jdbc:postgresql://source:5432/source",
    "SOURCE_DB_USER": "writer",
    "SOURCE_DB_PASSWORD": "p@ss word",
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    "ANALYTICS_DB_USER": "owner",
    "ANALYTICS_DB_PASSWORD": "secret",
    "DUCKDB_EXTENSION_DIRECTORY": "/opt/extensions",
}


def observation(
    entity: str = "customers",
    *,
    change: Change | None = CHANGE,
    seconds: Mapping[Layer, float] | None = None,
) -> Observation:
    """An observation whose layers were seen `seconds` after the change's commit."""
    observed = seconds if seconds is not None else {}
    return Observation(
        entity=entity,
        key=7,
        change=change,
        observed_at={layer: COMMITTED_AT + round(s * 1000) for layer, s in observed.items()},
    )


def probe_round(number: int, *observations: Observation) -> ProbeRound:
    return ProbeRound(
        number=number,
        marker=-number,
        waited_seconds=1.5,
        committed_at=COMMITTED_AT + 5,
        observations=observations,
    )


ALL_LAYERS_SECONDS = {Layer.LANDING: 3, Layer.BRONZE: 13, Layer.SILVER: 23, Layer.GOLD: 33}


@dataclass
class FakeClock:
    """A clock that only moves when the code sleeps, or by `tick` on every read."""

    now: float = 1000.0
    tick: float = 0.0
    slept: list[float] = field(default_factory=list)

    def clock(self) -> float:
        value = self.now
        self.now += self.tick
        return value

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def timing(self, random: float = 0.5) -> Timing:
        return Timing(clock=self.clock, sleep=self.sleep, random=lambda: random)


@dataclass
class FakeSource:
    lowest: dict[str, int | None] = field(default_factory=dict)
    updated: int | None = 1
    marks: list[tuple[tuple[str, ...], int]] = field(default_factory=list)
    asked: list[tuple[str, ...]] = field(default_factory=list)

    def lowest_keys(self, tables: Sequence[SourceTable]) -> dict[str, int | None]:
        self.asked.append(tuple(table.name for table in tables))
        return {table.name: self.lowest.get(table.name) for table in tables}

    def mark(self, targets: Sequence[ProbeTarget], marker: int) -> dict[str, int | None]:
        self.marks.append((tuple(target.name for target in targets), marker))
        return {target.name: self.updated for target in targets}


@dataclass
class FakeLanding:
    """Lands the change on the `after`-th search after each mark."""

    after: int | None = 1
    change: Change = CHANGE
    searches: int = 0
    marks: int = 0
    markers: list[int] = field(default_factory=list)

    def mark(self) -> None:
        self.marks += 1
        self.searches = 0

    def find(self, target: ProbeTarget, marker: int) -> Change | None:
        self.searches += 1
        self.markers.append(marker)
        if self.after is None or self.searches < self.after:
            return None
        return self.change


@dataclass
class FakeLookup:
    """Holds the change from the `after`-th check on, counting from the first check."""

    after: int | None = 1
    checks: int = 0
    on_check: Callable[[], None] = lambda: None

    def visible(self, target: ProbeTarget, change: Change) -> bool:
        self.checks += 1
        self.on_check()
        return self.after is not None and self.checks >= self.after


@dataclass
class MemoryStore:
    files: dict[str, str] = field(default_factory=dict)

    def write_text(self, path: str, text: str) -> None:
        self.files[path] = text

    def list_files(self, folder: str, suffix: str) -> list[str]:
        return sorted(
            path
            for path in self.files
            if path.rsplit("/", 1)[0] == folder and path.endswith(suffix)
        )

    def read_text(self, path: str) -> str:
        return self.files[path]
