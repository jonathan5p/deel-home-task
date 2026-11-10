from typing import Any

from fastapi import APIRouter

from api.models import (
    ErrorDetail,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)
from api.repository import MetricsRepository

# FastAPI's own type for extra responses: the values are free-form OpenAPI fields.
type Responses = dict[int | str, dict[str, Any]]

UNAVAILABLE: Responses = {
    503: {"model": ErrorDetail, "description": "The analytics database is unavailable"}
}


def metrics_router(repository: MetricsRepository) -> APIRouter:
    router = APIRouter(prefix="/metrics", tags=["metrics"], responses=UNAVAILABLE)

    @router.get(
        "/open-orders-by-delivery-date-and-status",
        summary="Number of open orders by delivery date and status",
    )
    async def open_orders_by_delivery_date_and_status() -> list[OpenOrdersByDeliveryDateAndStatus]:
        """One row per delivery date and open status, ordered by delivery date and status."""
        return await repository.open_orders_by_delivery_date_and_status()

    @router.get(
        "/top-delivery-dates-by-open-orders",
        summary="Top 3 delivery dates with more open orders",
    )
    async def top_delivery_dates_by_open_orders() -> list[TopDeliveryDateByOpenOrders]:
        """Ties for third place are included, so more than three rows can come back; ordered by
        open orders, most first, then delivery date."""
        return await repository.top_delivery_dates_by_open_orders()

    @router.get("/pending-items-by-product", summary="Number of open pending items by product")
    async def pending_items_by_product() -> list[PendingItemsByProduct]:
        """The items of open orders, counted and their quantities summed by product, ordered by
        product."""
        return await repository.pending_items_by_product()

    @router.get(
        "/top-customers-by-pending-orders",
        summary="Top 3 customers with more pending orders",
    )
    async def top_customers_by_pending_orders() -> list[TopCustomerByPendingOrders]:
        """Ties for third place are included; ordered by pending orders, most first, then
        customer. A customer not in the customer dimension yet has no name."""
        return await repository.top_customers_by_pending_orders()

    return router
