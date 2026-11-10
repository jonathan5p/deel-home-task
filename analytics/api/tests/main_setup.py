from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from analytics.api.tests.postgres_setup import FakeConnection, FakePool
from api.settings import ApiSettings

DATABASE = {
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    "ANALYTICS_DB_USER": "reader",
    "ANALYTICS_DB_PASSWORD": "secret",
}


@dataclass
class ManagedFakePool:
    """Records how the app opens and closes it, and answers every query with one row."""

    events: list[str] = field(default_factory=list)
    queries: FakePool = field(default_factory=lambda: FakePool(rows=[{"reachable": 1}]))

    async def open(self, wait: bool = False, timeout: float = 30.0) -> None:
        self.events.append(f"open wait={wait}")

    async def close(self, timeout: float = 5.0) -> None:
        self.events.append("close")

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncGenerator[FakeConnection]:
        async with self.queries.connection(timeout) as connection:
            yield connection


@dataclass
class PoolFactory:
    pool: ManagedFakePool = field(default_factory=ManagedFakePool)
    settings: list[ApiSettings] = field(default_factory=list)

    def __call__(self, settings: ApiSettings) -> ManagedFakePool:
        self.settings.append(settings)
        return self.pool
