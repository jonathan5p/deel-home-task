import json
import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field

from pyspark import SparkContext

LOGGER = "pipelines.lake.gold"
BATCH_PREFIX = "GOLD BATCH "
JOB_GROUP = "spark.jobGroup.id"

type JobIds = Callable[[], frozenset[int]]
type Clock = Callable[[], float]


def spark_jobs(context: SparkContext) -> JobIds:
    """The ids of the Spark jobs of the calling thread's job group that the status tracker still
    holds. A streaming query runs every micro-batch in a job group of its own, so a batch's jobs
    are told apart from those of the other streams running at the same time."""
    tracker = context.statusTracker()

    def ids() -> frozenset[int]:
        return frozenset(tracker.getJobIdsForGroup(context.getLocalProperty(JOB_GROUP)))

    return ids


def no_jobs() -> frozenset[int]:
    return frozenset()


@dataclass(frozen=True, slots=True)
class Step:
    name: str
    seconds: float
    jobs: int


@dataclass
class BatchRun:
    """The steps of one gold batch, each with its wall time and the Spark jobs it ran, and the
    rows written to each table."""

    load: str
    batch_id: int | None
    jobs: JobIds = no_jobs
    clock: Clock = time.perf_counter
    steps: list[Step] = field(default_factory=list)
    written: dict[str, int] = field(default_factory=dict)
    started: float = field(init=False)
    first_jobs: frozenset[int] = field(init=False)

    def __post_init__(self) -> None:
        self.started = self.clock()
        self.first_jobs = self.jobs()

    @contextmanager
    def step(self, name: str) -> Generator[None]:
        before = self.jobs()
        started = self.clock()
        try:
            yield
        finally:
            self.steps.append(Step(name, self.clock() - started, len(self.jobs() - before)))

    def summary(self) -> dict[str, object]:
        """The batch as one JSON-ready record: totals first, then every step in order."""
        return {
            "load": self.load,
            "batch_id": self.batch_id,
            "seconds": round(self.clock() - self.started, 3),
            "jobs": len(self.jobs() - self.first_jobs),
            "written": dict(self.written),
            "steps": [
                {"step": step.name, "seconds": round(step.seconds, 3), "jobs": step.jobs}
                for step in self.steps
            ],
        }

    def log(self, logger: logging.Logger | None = None) -> None:
        line = json.dumps(self.summary(), separators=(",", ":"))
        (logger or logging.getLogger(LOGGER)).info("%s%s", BATCH_PREFIX, line)
