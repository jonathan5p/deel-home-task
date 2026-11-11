from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class FakeClock:
    """A clock that only moves when the scheduler sleeps."""

    now: float = 0.0
    slept: list[float] = field(default_factory=list)

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def failing_once(action: Callable[[], object]) -> Callable[[], object]:
    failed: list[bool] = []

    def run() -> object:
        if not failed:
            failed.append(True)
            raise OSError("unreachable")
        return action()

    return run
