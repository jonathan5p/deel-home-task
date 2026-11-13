from dataclasses import dataclass, field


@dataclass
class FakeJobs:
    """The job ids a status tracker holds, which tests add to as jobs run."""

    ids: set[int] = field(default_factory=set)

    def __call__(self) -> frozenset[int]:
        return frozenset(self.ids)

    def run(self, *ids: int) -> None:
        self.ids.update(ids)


@dataclass
class Ticks:
    """A clock that moves one second every time it is read."""

    now: float = 0.0

    def __call__(self) -> float:
        self.now += 1.0
        return self.now
