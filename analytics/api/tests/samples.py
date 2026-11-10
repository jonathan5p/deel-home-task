from datetime import UTC, date, datetime

from api.models import (
    EntityFreshness,
    Freshness,
    OpenOrdersByDeliveryDateAndStatus,
    PendingItemsByProduct,
    TopCustomerByPendingOrders,
    TopDeliveryDateByOpenOrders,
)

OPEN_ORDERS = [
    OpenOrdersByDeliveryDateAndStatus(
        delivery_date=date(2026, 11, 10), status_code="PENDING", open_orders=2
    ),
    OpenOrdersByDeliveryDateAndStatus(delivery_date=None, status_code="UNKNOWN", open_orders=1),
]

TOP_DELIVERY_DATES = [
    TopDeliveryDateByOpenOrders(delivery_date=date(2026, 12, 1), open_orders=3),
    TopDeliveryDateByOpenOrders(delivery_date=date(2026, 12, 2), open_orders=2),
    TopDeliveryDateByOpenOrders(delivery_date=date(2026, 12, 3), open_orders=1),
    TopDeliveryDateByOpenOrders(delivery_date=date(2026, 12, 4), open_orders=1),
]

PENDING_ITEMS = [
    PendingItemsByProduct(product_id=10, pending_items=2, pending_quantity=10),
    PendingItemsByProduct(product_id=None, pending_items=1, pending_quantity=None),
]

TOP_CUSTOMERS = [
    TopCustomerByPendingOrders(customer_id=1, customer_name="Ada Lovelace", pending_orders=3),
    TopCustomerByPendingOrders(customer_id=4, customer_name=None, pending_orders=1),
]

CHECKED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)

FRESHNESS = Freshness(
    checked_at=CHECKED_AT,
    entities=[
        EntityFreshness(
            entity="orders",
            last_changed_at=datetime(2026, 11, 10, 11, 59, 30, tzinfo=UTC),
            lag_seconds=30.0,
        ),
        EntityFreshness(entity="products", last_changed_at=None, lag_seconds=None),
    ],
)
