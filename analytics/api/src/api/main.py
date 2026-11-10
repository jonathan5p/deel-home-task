import os
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from api.app import create_app
from api.postgres import Pool, PostgresMetricsRepository
from api.settings import ApiSettings


class ManagedPool(Pool, Protocol):
    async def open(self, wait: bool = False, timeout: float = 30.0) -> None: ...

    async def close(self, timeout: float = 5.0) -> None: ...


type PoolFactory = Callable[[ApiSettings], ManagedPool]


async def check_connection(connection: AsyncConnection[DictRow]) -> None:
    """Run before handing out a connection, so one broken by a database restart is replaced
    instead of failing a request."""
    await connection.execute("SELECT 1")


def create_pool(settings: ApiSettings) -> AsyncConnectionPool[AsyncConnection[DictRow]]:
    return AsyncConnectionPool(
        settings.db.conninfo(),
        connection_class=AsyncConnection[DictRow],
        kwargs={
            "user": settings.db.user,
            "password": settings.db.password,
            "autocommit": True,
            "row_factory": dict_row,
        },
        min_size=settings.pool.min_size,
        max_size=settings.pool.max_size,
        timeout=float(settings.pool.timeout_seconds),
        check=check_connection,
        open=False,
        name="gold",
    )


def create_app_from_settings(
    settings: ApiSettings, pool_factory: PoolFactory = create_pool
) -> FastAPI:
    pool = pool_factory(settings)

    # Opening without waiting lets the API start, and report itself unhealthy, while the
    # database is still down; the pool keeps trying to connect in the background.
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await pool.open(wait=False)
        try:
            yield
        finally:
            await pool.close()

    return create_app(PostgresMetricsRepository(pool), lifespan)


def create_app_from_env() -> FastAPI:
    return create_app_from_settings(ApiSettings.from_env(os.environ))
