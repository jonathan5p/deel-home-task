from typing import Protocol

from api.models import (
    Freshness,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)


class RepositoryUnavailableError(Exception):
    """The analytics database cannot answer now; the message is safe to show to clients."""


class MetricsRepository(Protocol):
    """Reads the gold views; rows come back in the order the business questions report them."""

    async def ping(self) -> None: ...

    async def freshness(self) -> Freshness: ...

    async def open_orders_by_delivery_date_and_status(
        self,
    ) -> list[OpenOrdersByDeliveryDateAndStatus]: ...

    async def top_delivery_dates_by_open_orders(self) -> list[TopDeliveryDateByOpenOrders]: ...

    async def pending_items_by_product(self) -> list[PendingItemsByProduct]: ...

    async def top_customers_by_pending_orders(self) -> list[TopCustomerByPendingOrders]: ...
