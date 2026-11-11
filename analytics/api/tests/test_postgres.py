import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import pytest
from psycopg import errors
from psycopg_pool import PoolTimeout

from analytics.api.tests.postgres_setup import (
    CHECKED_AT,
    EMPTY_FRESHNESS_ROWS,
    FRESHNESS_ROWS,
    OPEN_ORDER_ROWS,
    PENDING_ITEM_ROWS,
    TOP_CUSTOMER_ROWS,
    TOP_DELIVERY_DATE_ROWS,
    FakePool,
    normalized,
)
from api.models import (
    EntityFreshness,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    Row,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)
from api.postgres import GOLD_UNREADABLE, UNAVAILABLE, PostgresAnalyticsRepository
from api.repository import RepositoryUnavailableError

type Method = Callable[[PostgresAnalyticsRepository], Awaitable[object]]

METRICS: list[tuple[Method, list[dict[str, object]], type[Row], str]] = [
    (
        PostgresAnalyticsRepository.open_orders_by_delivery_date_and_status,
        OPEN_ORDER_ROWS,
        OpenOrdersByDeliveryDateAndStatus,
        "SELECT delivery_date, status_code, open_orders"
        " FROM gold.open_orders_by_delivery_date_and_status"
        " ORDER BY delivery_date, status_code",
    ),
    (
        PostgresAnalyticsRepository.top_delivery_dates_by_open_orders,
        TOP_DELIVERY_DATE_ROWS,
        TopDeliveryDateByOpenOrders,
        "SELECT delivery_date, open_orders FROM gold.top_delivery_dates_by_open_orders"
        " ORDER BY open_orders DESC, delivery_date",
    ),
    (
        PostgresAnalyticsRepository.pending_items_by_product,
        PENDING_ITEM_ROWS,
        PendingItemsByProduct,
        "SELECT product_id, pending_items, pending_quantity FROM gold.pending_items_by_product"
        " ORDER BY product_id",
    ),
    (
        PostgresAnalyticsRepository.top_customers_by_pending_orders,
        TOP_CUSTOMER_ROWS,
        TopCustomerByPendingOrders,
        "SELECT customer_id, customer_name, pending_orders"
        " FROM gold.top_customers_by_pending_orders"
        " ORDER BY pending_orders DESC, customer_id",
    ),
]
EVERY_METHOD: list[Method] = [
    PostgresAnalyticsRepository.ping,
    PostgresAnalyticsRepository.freshness,
    *(method for method, *_ in METRICS),
]


@pytest.mark.parametrize(("method", "rows", "model", "query"), METRICS)
def test_each_metric_reads_its_gold_view_in_the_reported_order(
    method: Method, rows: list[dict[str, object]], model: type[Row], query: str
) -> None:
    pool = FakePool(rows=rows)

    answer = asyncio.run(method(PostgresAnalyticsRepository(pool)))

    assert answer == [model.model_validate(row) for row in rows]
    assert [normalized(sent) for sent in pool.queries] == [query]


def test_freshness_reads_the_freshness_view_with_the_database_clock() -> None:
    pool = FakePool(rows=FRESHNESS_ROWS)

    answer = asyncio.run(PostgresAnalyticsRepository(pool).freshness())

    assert answer.checked_at == CHECKED_AT
    assert answer.entities == [EntityFreshness.model_validate(row) for row in FRESHNESS_ROWS]
    query = normalized(pool.queries[0])
    assert "now() AS checked_at" in query
    assert "LEFT JOIN gold.data_freshness" in query
    assert query.endswith("ORDER BY f.entity")


def test_freshness_still_has_a_clock_when_the_view_has_no_rows() -> None:
    answer = asyncio.run(
        PostgresAnalyticsRepository(FakePool(rows=EMPTY_FRESHNESS_ROWS)).freshness()
    )

    assert answer.checked_at == CHECKED_AT
    assert answer.entities == []


def test_ping_runs_a_query_through_the_pool() -> None:
    pool = FakePool(rows=[{"reachable": 1}])

    asyncio.run(PostgresAnalyticsRepository(pool).ping())

    assert pool.queries == ["SELECT 1 AS reachable"]


@pytest.mark.parametrize("method", EVERY_METHOD)
@pytest.mark.parametrize(
    "error",
    [PoolTimeout("no connection"), psycopg.OperationalError("connection refused")],
)
def test_a_database_out_of_reach_makes_the_repository_unavailable(
    method: Method, error: Exception
) -> None:
    repository = PostgresAnalyticsRepository(FakePool(connect_error=error))

    with pytest.raises(RepositoryUnavailableError, match=UNAVAILABLE) as raised:
        asyncio.run(method(repository))

    assert raised.value.__cause__ is error


def test_a_statement_timeout_makes_the_repository_unavailable() -> None:
    pool = FakePool(query_error=errors.QueryCanceled("canceling statement due to timeout"))

    with pytest.raises(RepositoryUnavailableError, match=UNAVAILABLE):
        asyncio.run(PostgresAnalyticsRepository(pool).pending_items_by_product())


@pytest.mark.parametrize(
    "error",
    [
        errors.UndefinedTable("relation does not exist"),
        errors.InsufficientPrivilege("permission denied"),
    ],
)
def test_gold_views_it_cannot_read_make_the_repository_unavailable(error: Exception) -> None:
    pool = FakePool(query_error=error)

    with pytest.raises(RepositoryUnavailableError, match=GOLD_UNREADABLE):
        asyncio.run(PostgresAnalyticsRepository(pool).top_customers_by_pending_orders())


def test_any_other_database_error_is_left_to_the_caller() -> None:
    pool = FakePool(query_error=errors.SyntaxError("syntax error"))

    with pytest.raises(errors.SyntaxError):
        asyncio.run(PostgresAnalyticsRepository(pool).pending_items_by_product())
