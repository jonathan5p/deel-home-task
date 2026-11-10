from dataclasses import dataclass, field

from analytics.api.tests.samples import (
    FRESHNESS,
    OPEN_ORDERS,
    PENDING_ITEMS,
    TOP_CUSTOMERS,
    TOP_DELIVERY_DATES,
)
from api.models import (
    Freshness,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)


@dataclass
class FakeRepository:
    """Answers with the sample rows, or raises `error` from every method when it is set."""

    open_orders: list[OpenOrdersByDeliveryDateAndStatus] = field(
        default_factory=lambda: list(OPEN_ORDERS)
    )
    top_delivery_dates: list[TopDeliveryDateByOpenOrders] = field(
        default_factory=lambda: list(TOP_DELIVERY_DATES)
    )
    pending_items: list[PendingItemsByProduct] = field(default_factory=lambda: list(PENDING_ITEMS))
    top_customers: list[TopCustomerByPendingOrders] = field(
        default_factory=lambda: list(TOP_CUSTOMERS)
    )
    current_freshness: Freshness = FRESHNESS
    error: Exception | None = None

    def _check(self) -> None:
        if self.error is not None:
            raise self.error

    async def ping(self) -> None:
        self._check()

    async def freshness(self) -> Freshness:
        self._check()
        return self.current_freshness

    async def open_orders_by_delivery_date_and_status(
        self,
    ) -> list[OpenOrdersByDeliveryDateAndStatus]:
        self._check()
        return self.open_orders

    async def top_delivery_dates_by_open_orders(self) -> list[TopDeliveryDateByOpenOrders]:
        self._check()
        return self.top_delivery_dates

    async def pending_items_by_product(self) -> list[PendingItemsByProduct]:
        self._check()
        return self.pending_items

    async def top_customers_by_pending_orders(self) -> list[TopCustomerByPendingOrders]:
        self._check()
        return self.top_customers
