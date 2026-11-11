import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC
from pathlib import Path
from typing import LiteralString, cast

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from api.main import create_pool
from api.models import Row
from api.postgres import PostgresAnalyticsRepository
from api.settings import ApiSettings

QUERIES = Path(__file__).resolve().parents[3] / "analytics/sql/gold"

type Metric = Callable[[], Awaitable[Sequence[Row]]]


async def sql_rows(pool: AsyncConnectionPool[AsyncConnection[DictRow]], query: str) -> list[object]:
    async with pool.connection() as connection:
        cursor = await connection.execute(cast("LiteralString", query))
        return [json.loads(json.dumps(row, default=str)) for row in await cursor.fetchall()]


async def answers() -> dict[str, dict[str, list[object]]]:
    """Per business question, the repository's rows and the rows of its query file; and the
    freshness the repository reports next to gold.data_freshness, both as naive UTC times."""
    pool = create_pool(ApiSettings.from_env(os.environ))
    await pool.open(wait=True)
    try:
        repository = PostgresAnalyticsRepository(pool)
        metrics: dict[str, Metric] = {
            "01_open_orders_by_delivery_date_and_status.sql": (
                repository.open_orders_by_delivery_date_and_status
            ),
            "02_top_delivery_dates_by_open_orders.sql": (
                repository.top_delivery_dates_by_open_orders
            ),
            "03_pending_items_by_product.sql": repository.pending_items_by_product,
            "04_top_customers_by_pending_orders.sql": repository.top_customers_by_pending_orders,
        }
        result: dict[str, dict[str, list[object]]] = {}
        for name, metric in metrics.items():
            result[name] = {
                "api": [row.model_dump(mode="json") for row in await metric()],
                "sql": await sql_rows(pool, (QUERIES / name).read_text()),
            }
        freshness = await repository.freshness()
        result["freshness"] = {
            "api": [
                [
                    entity.entity,
                    entity.last_changed_at
                    and entity.last_changed_at.astimezone(UTC).replace(tzinfo=None).isoformat(" "),
                    entity.lag_seconds is not None and entity.lag_seconds >= 0,
                ]
                for entity in freshness.entities
            ],
            "sql": [
                [row["entity"], row["last_changed_at"], row["last_changed_at"] is not None]
                for row in await sql_rows(pool, "SELECT * FROM gold.data_freshness ORDER BY entity")
                if isinstance(row, dict)
            ],
        }
        return result
    finally:
        await pool.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(answers())))
