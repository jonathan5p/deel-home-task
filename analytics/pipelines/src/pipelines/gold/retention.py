import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import LiteralString, cast

import psycopg

from pipelines.gold.postgres import (
    WRITER_LOCK,
    Connect,
    ConnectionFactory,
    connection_factory,
    transaction,
)
from pipelines.settings import GoldRetentionSettings


def retention_query(settings: GoldRetentionSettings, as_of: date) -> str:
    return (
        "SELECT dropped FROM gold_internal.apply_history_retention("
        f"{settings.keep_months}, {settings.months_ahead}, DATE '{as_of.isoformat()}') AS dropped"
    )


def apply_retention(
    connect: ConnectionFactory, settings: GoldRetentionSettings, as_of: date
) -> list[str]:
    """Partition the history months the default partition holds and the months ahead, then drop
    the partitions that end before the window; returns the dropped partitions."""
    with transaction(connect) as connection:
        connection.execute(WRITER_LOCK)
        # Built from validated integers and a date, never from free text.
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
