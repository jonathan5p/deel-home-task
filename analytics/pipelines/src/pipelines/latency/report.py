import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.measure import (
    Breach,
    Hop,
    Stats,
    Timeout,
    breaches,
    hop_stats,
    hops,
    last_layer,
    latencies,
    latency_stats,
    timeouts,
)
from pipelines.latency.probe import MARKER_COLUMN, Observation, ProbeRound, ProbeTarget
from pipelines.latency.settings import ProbeOptions
from pipelines.settings import LakeSettings

REPORTS = "latency/reports"
INDEX = "index.html"
REPORT_TIME_FORMAT = "%Y%m%dT%H%M%SZ"


def iso_time(epoch_millis: int) -> str:
    moment = datetime.fromtimestamp(epoch_millis / 1000, UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class RunReport:
    """One run of the probe: its settings, targets and rounds, with what they add up to."""

    started_at: int
    finished_at: int
    options: ProbeOptions
    trigger_seconds: Mapping[Layer, int]
    targets: Sequence[ProbeTarget]
    rounds: Sequence[ProbeRound]

    @property
    def latency(self) -> dict[str, dict[Layer, Stats | None]]:
        return latency_stats(self.rounds)

    @property
    def hops(self) -> dict[str, dict[Hop, Stats | None]]:
        return hop_stats(self.rounds)

    @property
    def breaches(self) -> list[Breach]:
        return breaches(self.rounds, self.options.max_latency)

    @property
    def timeouts(self) -> list[Timeout]:
        return timeouts(self.rounds)

    @property
    def passed(self) -> bool:
        return not self.breaches and not self.timeouts

    @property
    def keys(self) -> dict[str, int]:
        return {target.name: target.key for target in self.targets}


def _stats[K: str](values: Mapping[K, Stats | None]) -> dict[str, object]:
    return {
        str(name): None if value is None else value.as_object() for name, value in values.items()
    }


def _rounded[K: str](values: Mapping[K, float | None]) -> dict[str, float | None]:
    return {str(name): None if value is None else round(value, 3) for name, value in values.items()}


def observation_object(observation: Observation) -> dict[str, object]:
    change = observation.change
    reached = last_layer(observation)
    return {
        "key": observation.key,
        "commit_sequence": None if change is None else change.commit_sequence,
        "changed_at": None if change is None else iso_time(change.changed_at),
        "captured_at": None if change is None else iso_time(change.captured_at),
        "published_at": None if change is None else iso_time(change.published_at),
        "observed_at": {
            str(layer): None
            if layer not in observation.observed_at
            else iso_time(observation.observed_at[layer])
            for layer in LAYERS
        },
        "latency_seconds": _rounded(latencies(observation)),
        "hop_seconds": _rounded(hops(observation)),
        "last_layer": None if reached is None else str(reached),
    }


def summary_object(report: RunReport) -> dict[str, object]:
    return {
        "passed": report.passed,
        "probes": len(report.rounds),
        "timeouts": [timeout.as_object() for timeout in report.timeouts],
        "breaches": [breach.as_object() for breach in report.breaches],
        "latency_seconds": {e: _stats(by_layer) for e, by_layer in report.latency.items()},
        "hop_seconds": {e: _stats(by_hop) for e, by_hop in report.hops.items()},
    }


def report_document(report: RunReport) -> dict[str, object]:
    """The run's settings, its summary, then every probe's change and observations."""
    options = report.options
    return {
        "run": {
            "started_at": iso_time(report.started_at),
            "finished_at": iso_time(report.finished_at),
            "entities": list(options.entities),
            "probes": options.probes,
            "poll_seconds": options.poll_seconds,
            "timeout_seconds": options.timeout_seconds,
            "trigger_seconds": {str(layer): s for layer, s in report.trigger_seconds.items()},
            "max_latency_seconds": {str(layer): s for layer, s in options.max_latency.items()},
            "marker_column": MARKER_COLUMN,
            "keys": report.keys,
        },
        "summary": summary_object(report),
        "probes": [
            {
                "probe": probe.number,
                "marker": probe.marker,
                "waited_seconds": round(probe.waited_seconds, 3),
                "committed_at": iso_time(probe.committed_at),
                "entities": {o.entity: observation_object(o) for o in probe.observations},
            }
            for probe in report.rounds
        ],
    }


@dataclass(frozen=True, slots=True)
class ReportPaths:
    json: str
    html: str


def reports_folder(lake: LakeSettings) -> str:
    return f"{lake.root}/{REPORTS}"


def report_paths(lake: LakeSettings, now: datetime) -> ReportPaths:
    stem = f"{reports_folder(lake)}/{now.astimezone(UTC):{REPORT_TIME_FORMAT}}"
    return ReportPaths(json=f"{stem}.json", html=f"{stem}.html")


def index_path(lake: LakeSettings) -> str:
    return f"{reports_folder(lake)}/{INDEX}"


@dataclass(frozen=True, slots=True)
class IndexEntry:
    """One run in the reports index, read back from its JSON report."""

    name: str
    started_at: str
    passed: bool
    probes: int
    entities: tuple[str, ...]
    timeouts: int
    breaches: int

    @property
    def html(self) -> str:
        return f"{self.name}.html"


def index_entry(name: str, document: object) -> IndexEntry | None:
    """The index entry of a JSON report, or None if it is not a latency report."""
    if not isinstance(document, dict):
        return None
    run, summary = document.get("run"), document.get("summary")
    if not isinstance(run, dict) or not isinstance(summary, dict):
        return None
    started_at, entities = run.get("started_at"), run.get("entities")
    passed, probes = summary.get("passed"), summary.get("probes")
    found_timeouts, found_breaches = summary.get("timeouts"), summary.get("breaches")
    if not (
        isinstance(started_at, str)
        and isinstance(entities, list)
        and isinstance(passed, bool)
        and isinstance(probes, int)
        and isinstance(found_timeouts, list)
        and isinstance(found_breaches, list)
    ):
        return None
    return IndexEntry(
        name=name,
        started_at=started_at,
        passed=passed,
        probes=probes,
        entities=tuple(str(entity) for entity in entities),
        timeouts=len(found_timeouts),
        breaches=len(found_breaches),
    )


class TextStore(Protocol):
    def write_text(self, path: str, text: str) -> None: ...

    def list_files(self, folder: str, suffix: str) -> list[str]:
        """The paths of the files directly in `folder` whose names end with `suffix`."""
        ...

    def read_text(self, path: str) -> str: ...


def index_entries(store: TextStore, lake: LakeSettings) -> list[IndexEntry]:
    """Every latency report under the lake, newest first; files that do not parse as one are
    left out."""
    entries = []
    for path in store.list_files(reports_folder(lake), ".json"):
        name = path.rsplit("/", 1)[-1].removesuffix(".json")
        try:
            document = json.loads(store.read_text(path))
        except json.JSONDecodeError:
            continue
        entry = index_entry(name, document)
        if entry is not None:
            entries.append(entry)
    return sorted(entries, key=lambda entry: entry.name, reverse=True)
