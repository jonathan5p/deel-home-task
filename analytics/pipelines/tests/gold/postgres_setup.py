from dataclasses import dataclass, field
from typing import LiteralString


class StatementFailedError(RuntimeError):
    pass


@dataclass
class FakeConnection:
    """Records what the writer does with a connection; `fail_on` makes that statement fail."""

    fail_on: str | None = None
    log: list[str] = field(default_factory=list)

    def execute(self, query: LiteralString) -> object:
        self.log.append(f"execute {query}")
        if query == self.fail_on:
            raise StatementFailedError(query)
        return None

    def commit(self) -> None:
        self.log.append("commit")

    def rollback(self) -> None:
        self.log.append("rollback")

    def close(self) -> None:
        self.log.append("close")
