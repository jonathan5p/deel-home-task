from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import LiteralString

from api.postgres import Cursor

type Rows = list[dict[str, object]]

OPEN_ORDER_ROWS: Rows = [
    {"delivery_date": date(2026, 11, 10), "status_code": "PENDING", "open_orders": 2},
    {"delivery_date": None, "status_code": "UNKNOWN", "open_orders": 1},
]
TOP_DELIVERY_DATE_ROWS: Rows = [{"delivery_date": date(2026, 12, 1), "open_orders": 3}]
PENDING_ITEM_ROWS: Rows = [{"product_id": 10, "pending_items": 2, "pending_quantity": None}]
TOP_CUSTOMER_ROWS: Rows = [{"customer_id": 4, "customer_name": None, "pending_orders": 1}]

CHECKED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
FRESHNESS_ROWS: Rows = [
    {
        "checked_at": CHECKED_AT,
        "entity": "orders",
        "last_changed_at": datetime(2026, 11, 10, 11, 59, 30, tzinfo=UTC),
        "lag_seconds": 30.0,
    },
    {"checked_at": CHECKED_AT, "entity": "products", "last_changed_at": None, "lag_seconds": None},
]
EMPTY_FRESHNESS_ROWS: Rows = [
    {"checked_at": CHECKED_AT, "entity": None, "last_changed_at": None, "lag_seconds": None}
]


@dataclass
class FakeCursor:
    rows: Sequence[Mapping[str, object]]

    async def fetchall(self) -> Sequence[Mapping[str, object]]:
        return self.rows


@dataclass
class FakeConnection:
    pool: FakePool

    async def execute(self, query: LiteralString) -> Cursor:
        self.pool.queries.append(query)
        if self.pool.query_error is not None:
            raise self.pool.query_error
        return FakeCursor(self.pool.rows)


@dataclass
class FakePool:
    """Hands out one fake connection; `connect_error` fails the checkout, `query_error` the
    query."""

    rows: Sequence[Mapping[str, object]] = ()
    connect_error: Exception | None = None
    query_error: Exception | None = None
    queries: list[str] = field(default_factory=list)

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncGenerator[FakeConnection]:
        if self.connect_error is not None:
            raise self.connect_error
        yield FakeConnection(self)


def normalized(query: str) -> str:
    return " ".join(query.split())
