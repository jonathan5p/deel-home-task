import json
import logging
import os
import time
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    disable_created_metrics,
    start_http_server,
)

from pipelines.observability.schedule import Task, run_tasks
from pipelines.settings import StageExporterSettings

# The History Server's REST API answers with untyped JSON.
type Json = Any
type FetchJson = Callable[[str], Json]

LABELS = ("app", "query")
QUANTILES = "0,0.5,0.95,1"
MAX, MEDIAN = 3, 1
DURATION_BUCKETS = (0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)
TASKS_BUCKETS = (1, 2, 4, 8, 16, 32, 64, 128, 200, 256, 512, 1024, 4096)
SKEW_BUCKETS = (1, 1.25, 1.5, 2, 3, 5, 10, 20, 50, 100)
MEDIAN_TASK_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def epoch_seconds(value: str) -> float:
    """A History Server time, such as `2026-11-11T20:08:40.439GMT`, as epoch seconds."""
    return datetime.strptime(value.removesuffix("GMT"), TIME_FORMAT).replace(tzinfo=UTC).timestamp()


@dataclass(frozen=True, slots=True)
class AppAttempt:
    app_id: str
    attempt_id: str | None
    name: str
    completed: bool

    @property
    def path(self) -> str:
        """The attempt's REST path; standalone client-mode applications have no attempt id."""
        suffix = f"/{self.attempt_id}" if self.attempt_id else ""
        return f"applications/{self.app_id}{suffix}"


def active_attempts(applications: Json, since: float) -> list[AppAttempt]:
    """The attempts that ran after `since`: the running ones and those that ended after it."""
    return [
        AppAttempt(app["id"], attempt.get("attemptId"), app["name"], attempt["completed"])
        for app in applications
        for attempt in app["attempts"]
        if not attempt["completed"] or attempt["endTimeEpoch"] / 1000 >= since
    ]


def query_name(stage: Json) -> str:
    """The streaming query a stage belongs to: Spark starts the description of every
    micro-batch job with the query's name. Empty for batch jobs."""
    description = stage.get("description") or ""
    return description.split("\n", 1)[0]


def ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


def task_duration_skew(summary: Json) -> float | None:
    """The slowest task's duration over the median task's."""
    durations = summary["duration"]
    return ratio(durations[MAX], durations[MEDIAN])


def data_skew(stage: Json, summary: Json) -> float | None:
    """The largest task's bytes read over the median task's: shuffle bytes when the stage reads
    a shuffle, input bytes otherwise. None when the median task reads nothing."""
    if stage["shuffleReadBytes"] > 0:
        read = summary["shuffleReadMetrics"]["readBytes"]
    else:
        read = summary["inputMetrics"]["bytesRead"]
    return ratio(read[MAX], read[MEDIAN])


def stage_seconds(stage: Json) -> float:
    return epoch_seconds(stage["completionTime"]) - epoch_seconds(stage["submissionTime"])


@dataclass(frozen=True, slots=True)
class StageCounter:
    """A counter fed from one field of the stage, scaled to its unit."""

    name: str
    documentation: str
    field: str
    scale: float = 1


STAGE_COUNTERS = (
    StageCounter(
        "spark_stage_run_seconds", "Task run time of completed stages.", "executorRunTime", 1e-3
    ),
    StageCounter(
        "spark_stage_cpu_seconds", "Task CPU time of completed stages.", "executorCpuTime", 1e-9
    ),
    StageCounter(
        "spark_stage_gc_seconds",
        "Task JVM garbage collection time of completed stages.",
        "jvmGcTime",
        1e-3,
    ),
    StageCounter(
        "spark_stage_input_bytes", "Bytes read from sources by completed stages.", "inputBytes"
    ),
    StageCounter(
        "spark_stage_input_records",
        "Records read from sources by completed stages.",
        "inputRecords",
    ),
    StageCounter(
        "spark_stage_output_bytes", "Bytes written to sinks by completed stages.", "outputBytes"
    ),
    StageCounter(
        "spark_stage_output_records",
        "Records written to sinks by completed stages.",
        "outputRecords",
    ),
    StageCounter(
        "spark_stage_shuffle_read_local_bytes",
        "Shuffle bytes read from the same executor's blocks.",
        "shuffleLocalBytesRead",
    ),
    StageCounter(
        "spark_stage_shuffle_read_remote_bytes",
        "Shuffle bytes read from other executors over the network.",
        "shuffleRemoteBytesRead",
    ),
    StageCounter(
        "spark_stage_shuffle_write_bytes",
        "Shuffle bytes written by completed stages.",
        "shuffleWriteBytes",
    ),
    StageCounter(
        "spark_stage_shuffle_fetch_wait_seconds",
        "Time tasks waited for shuffle blocks.",
        "shuffleFetchWaitTime",
        1e-3,
    ),
    StageCounter(
        "spark_stage_memory_spill_bytes",
        "Bytes spilled from memory, before compression.",
        "memoryBytesSpilled",
    ),
    StageCounter(
        "spark_stage_disk_spill_bytes",
        "Bytes spilled to disk, after compression.",
        "diskBytesSpilled",
    ),
)


@dataclass
class StageMetrics:
    """The Prometheus metrics of completed stages, labelled by application and streaming query."""

    registry: CollectorRegistry
    counters: dict[str, Counter] = field(init=False)
    duration: Histogram = field(init=False)
    tasks: Histogram = field(init=False)
    median_task: Histogram = field(init=False)
    task_skew: Histogram = field(init=False)
    data_skew: Histogram = field(init=False)

    def __post_init__(self) -> None:
        self.counters = {
            counter.field: Counter(
                counter.name, counter.documentation, LABELS, registry=self.registry
            )
            for counter in STAGE_COUNTERS
        }
        self.duration = self.histogram(
            "spark_stage_duration_seconds",
            "Time from a stage's submission to its completion.",
            DURATION_BUCKETS,
        )
        self.tasks = self.histogram("spark_stage_tasks", "Tasks per stage.", TASKS_BUCKETS)
        self.median_task = self.histogram(
            "spark_stage_median_task_seconds",
            "Duration of each stage's median task.",
            MEDIAN_TASK_BUCKETS,
        )
        self.task_skew = self.histogram(
            "spark_stage_task_duration_skew",
            "Each stage's slowest task duration over its median task duration.",
            SKEW_BUCKETS,
        )
        self.data_skew = self.histogram(
            "spark_stage_data_skew",
            "Each stage's largest task read over its median task read, in bytes.",
            SKEW_BUCKETS,
        )

    def histogram(self, name: str, documentation: str, buckets: Iterable[float]) -> Histogram:
        return Histogram(
            name, documentation, LABELS, buckets=tuple(buckets), registry=self.registry
        )

    def record(self, app: str, stage: Json, summary: Json) -> None:
        labels = {"app": app, "query": query_name(stage)}
        for counter in STAGE_COUNTERS:
            self.counters[counter.field].labels(**labels).inc(stage[counter.field] * counter.scale)
        self.duration.labels(**labels).observe(stage_seconds(stage))
        self.tasks.labels(**labels).observe(stage["numTasks"])
        self.median_task.labels(**labels).observe(summary["duration"][MEDIAN] / 1000)
        for histogram, value in (
            (self.task_skew, task_duration_skew(summary)),
            (self.data_skew, data_skew(stage, summary)),
        ):
            if value is not None:
                histogram.labels(**labels).observe(value)


@dataclass(frozen=True, slots=True)
class StageKey:
    app_path: str
    stage_id: int
    attempt_id: int


@dataclass
class StageExporter:
    """Records once each stage that completes after `started`, from the History Server REST API."""

    fetch: FetchJson
    metrics: StageMetrics
    started: float
    seen: set[StageKey] = field(default_factory=set)
    finished: set[str] = field(default_factory=set)

    def poll(self) -> int:
        """Record the newly completed stages; return how many were recorded."""
        recorded = 0
        for attempt in active_attempts(self.fetch("applications"), self.started):
            if attempt.path in self.finished:
                continue
            for stage in self.fetch(f"{attempt.path}/stages?status=complete"):
                key = StageKey(attempt.path, stage["stageId"], stage["attemptId"])
                if key in self.seen:
                    continue
                if epoch_seconds(stage["completionTime"]) >= self.started:
                    summary = self.fetch(
                        f"{attempt.path}/stages/{key.stage_id}/{key.attempt_id}"
                        f"/taskSummary?quantiles={QUANTILES}"
                    )
                    self.metrics.record(attempt.name, stage, summary)
                    recorded += 1
                self.seen.add(key)
            if attempt.completed:
                self.finished.add(attempt.path)
        return recorded


def http_fetch(base_url: str, timeout: float = 30) -> FetchJson:
    """A JSON getter for the REST API under `base_url`."""
    root = base_url.rstrip("/") + "/api/v1/"

    def fetch(path: str) -> Json:
        with urllib.request.urlopen(root + path, timeout=timeout) as response:
            return json.load(response)

    return fetch


def main(env: Mapping[str, str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    disable_created_metrics()
    settings = StageExporterSettings.from_env(env)
    registry = CollectorRegistry()
    exporter = StageExporter(
        http_fetch(settings.history_server_url), StageMetrics(registry), time.time()
    )
    start_http_server(settings.port, registry=registry)
    run_tasks([Task("completed stages recorded", settings.poll_seconds, exporter.poll)])


if __name__ == "__main__":
    main(os.environ)
