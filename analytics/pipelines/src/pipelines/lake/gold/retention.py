import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import LiteralString, cast

import psycopg

from pipelines.lake.gold.postgres import (
    WRITER_LOCK,
    Connect,
    ConnectionFactory,
    connection_factory,
    transaction,
)
from pipelines.settings import GoldRetentionSettings


def retained_from(keep_months: int, as_of: date) -> datetime:
    """The start of the history window retention keeps as of a day: the first day of the month
    `keep_months` months before the month of `as_of`. The months before it are dropped."""
    months = as_of.year * 12 + as_of.month - 1 - keep_months
    return datetime(months // 12, months % 12 + 1, 1)


def retention_query(settings: GoldRetentionSettings, as_of: date) -> str:
    return (
        "SELECT dropped FROM gold_internal.apply_history_retention("
        f"{settings.keep_months}, {settings.months_ahead}, DATE '{as_of.isoformat()}') AS dropped"
    )


def apply_retention(
    connect: ConnectionFactory, settings: GoldRetentionSettings, as_of: date
) -> list[str]:
    """The history partitions this run dropped."""
    with transaction(connect) as connection:
        connection.execute(WRITER_LOCK)
        dropped = connection.execute(
            cast("LiteralString", retention_query(settings, as_of))
        ).fetchall()
    return [str(name) for (name,) in dropped]


def main(env: Mapping[str, str], today: date, connect: Connect = psycopg.connect) -> None:
    settings = GoldRetentionSettings.from_env(env)
    dropped = apply_retention(connection_factory(settings.analytics_db, connect), settings, today)
    print(
        f"Gold retention as of {today.isoformat()}, keeping {settings.keep_months} months:"
        f" dropped {len(dropped)} partition(s)"
    )
    for name in dropped:
        print(f"  {name}")


if __name__ == "__main__":
    main(os.environ, datetime.now(UTC).date())
