from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import LiteralString


class StatementFailedError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FakeCursor:
    rows: list[tuple[object, ...]]

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


@dataclass
class FakeConnection:
    fail_on: str | None = None
    results: Mapping[str, list[tuple[object, ...]]] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)

    def execute(self, query: LiteralString) -> FakeCursor:
        self.log.append(f"execute {query}")
        if query == self.fail_on:
            raise StatementFailedError(query)
        return FakeCursor(list(self.results.get(query, [])))

    def commit(self) -> None:
        self.log.append("commit")

    def rollback(self) -> None:
        self.log.append("rollback")

    def close(self) -> None:
        self.log.append("close")
