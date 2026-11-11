import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class Task:
    name: str
    interval_seconds: float
    action: Callable[[], object]
    due: float = 0.0


def run_tasks(
    tasks: Sequence[Task],
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    rounds: int | None = None,
) -> None:
    """Run each task whenever it is due, forever or for `rounds` rounds. A failing task is
    logged and runs again at its next turn, so one unreachable service never stops the others."""
    done = 0
    while rounds is None or done < rounds:
        for task in tasks:
            now = clock()
            if now < task.due:
                continue
            try:
                log.info("%s: %s", task.name, task.action())
            except Exception:
                log.exception("%s failed", task.name)
            task.due = now + task.interval_seconds
        done += 1
        sleep(max(0.0, min(task.due for task in tasks) - clock()))
