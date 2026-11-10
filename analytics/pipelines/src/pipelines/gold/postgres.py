from collections.abc import Callable, Iterable
from typing import LiteralString, Protocol, cast

import psycopg

from common.settings import AnalyticsDbSettings

JDBC_PREFIX = "jdbc:"


class Connection(Protocol):
    """The part of a psycopg connection the gold writer uses."""

    def execute(self, query: LiteralString) -> object: ...

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


def run_in_transaction(connect: ConnectionFactory, statements: Iterable[str]) -> None:
    """Run the statements in order in one transaction on a new connection: all or none."""
    connection = connect()
    try:
        for statement in statements:
            # Built from schema identifiers, never from data, so nothing needs a parameter.
            connection.execute(cast("LiteralString", statement))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
