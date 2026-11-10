from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import LiteralString, Protocol, cast

import psycopg

from common.settings import AnalyticsDbSettings

JDBC_PREFIX = "jdbc:"
WRITER_LOCK = "SELECT pg_advisory_xact_lock(hashtextextended('gold_internal', 0))"


class Cursor(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...


class Connection(Protocol):
    """The part of a psycopg connection the gold writer uses."""

    def execute(self, query: LiteralString) -> Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


type ConnectionFactory = Callable[[], Connection]
type Connect = Callable[..., Connection]


def conninfo(url: str) -> str:
    """The libpq connection URI of a PostgreSQL JDBC URL (`jdbc:postgresql://host/db`)."""
    return url.removeprefix(JDBC_PREFIX)


def connection_factory(
    settings: AnalyticsDbSettings, connect: Connect = psycopg.connect
) -> ConnectionFactory:
    def factory() -> Connection:
        return connect(conninfo(settings.url), user=settings.user, password=settings.password)

    return factory


@contextmanager
def transaction(connect: ConnectionFactory) -> Generator[Connection]:
    """A new connection whose work in the block is committed, or rolled back if the block fails."""
    connection = connect()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def run_in_transaction(connect: ConnectionFactory, statements: Iterable[str]) -> None:
    """Run the statements in order in one transaction on a new connection: all or none."""
    with transaction(connect) as connection:
        for statement in statements:
            # Built from schema identifiers, never from data, so nothing needs a parameter.
            connection.execute(cast("LiteralString", statement))
