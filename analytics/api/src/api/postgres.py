from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import LiteralString, Protocol

import psycopg
from psycopg import errors

from api.models import (
    EntityFreshness,
    Freshness,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)
from api.repository import RepositoryUnavailableError

UNAVAILABLE = "The analytics database is unavailable"
GOLD_UNREADABLE = "The gold views cannot be read in the analytics database"

PING = "SELECT 1 AS reachable"

# Each order matches the business question's query in analytics/sql/gold/.
OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS = """
SELECT delivery_date, status_code, open_orders
FROM gold.open_orders_by_delivery_date_and_status
ORDER BY delivery_date, status_code
"""

TOP_DELIVERY_DATES_BY_OPEN_ORDERS = """
SELECT delivery_date, open_orders
FROM gold.top_delivery_dates_by_open_orders
ORDER BY open_orders DESC, delivery_date
"""

PENDING_ITEMS_BY_PRODUCT = """
SELECT product_id, pending_items, pending_quantity
FROM gold.pending_items_by_product
ORDER BY product_id
"""

TOP_CUSTOMERS_BY_PENDING_ORDERS = """
SELECT customer_id, customer_name, pending_orders
FROM gold.top_customers_by_pending_orders
ORDER BY pending_orders DESC, customer_id
"""

# Gold stores source commit times as UTC timestamps without a zone. The left join keeps the
# clock row when the view returns nothing.
FRESHNESS = """
WITH clock AS (SELECT now() AS checked_at)
SELECT
    clock.checked_at,
    f.entity,
    f.last_changed_at AT TIME ZONE 'UTC' AS last_changed_at,
    extract(epoch FROM clock.checked_at - (f.last_changed_at AT TIME ZONE 'UTC'))::float8
        AS lag_seconds
FROM clock
LEFT JOIN gold.data_freshness AS f ON true
ORDER BY f.entity
"""


class Cursor(Protocol):
    async def fetchall(self) -> Sequence[Mapping[str, object]]: ...


class Connection(Protocol):
    async def execute(self, query: LiteralString) -> Cursor: ...


class Pool(Protocol):
    """The part of a psycopg pool of dict-row connections the repository uses."""

    def connection(
        self, timeout: float | None = None
    ) -> AbstractAsyncContextManager[Connection]: ...


class PostgresMetricsRepository:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def _fetch(self, query: LiteralString) -> Sequence[Mapping[str, object]]:
        try:
            async with self._pool.connection() as connection:
                cursor = await connection.execute(query)
                return await cursor.fetchall()
        except (errors.UndefinedTable, errors.InsufficientPrivilege) as error:
            raise RepositoryUnavailableError(GOLD_UNREADABLE) from error
        except psycopg.OperationalError as error:
            raise RepositoryUnavailableError(UNAVAILABLE) from error

    async def ping(self) -> None:
        await self._fetch(PING)

    async def freshness(self) -> Freshness:
        rows = await self._fetch(FRESHNESS)
        return Freshness.model_validate(
            {
                "checked_at": rows[0]["checked_at"],
                "entities": [EntityFreshness.model_validate(row) for row in rows if row["entity"]],
            }
        )

    async def open_orders_by_delivery_date_and_status(
        self,
    ) -> list[OpenOrdersByDeliveryDateAndStatus]:
        rows = await self._fetch(OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS)
        return [OpenOrdersByDeliveryDateAndStatus.model_validate(row) for row in rows]

    async def top_delivery_dates_by_open_orders(self) -> list[TopDeliveryDateByOpenOrders]:
        rows = await self._fetch(TOP_DELIVERY_DATES_BY_OPEN_ORDERS)
        return [TopDeliveryDateByOpenOrders.model_validate(row) for row in rows]

    async def pending_items_by_product(self) -> list[PendingItemsByProduct]:
        rows = await self._fetch(PENDING_ITEMS_BY_PRODUCT)
        return [PendingItemsByProduct.model_validate(row) for row in rows]

    async def top_customers_by_pending_orders(self) -> list[TopCustomerByPendingOrders]:
        rows = await self._fetch(TOP_CUSTOMERS_BY_PENDING_ORDERS)
        return [TopCustomerByPendingOrders.model_validate(row) for row in rows]
