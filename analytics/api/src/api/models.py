from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class Row(BaseModel):
    model_config = ConfigDict(frozen=True)


class OpenOrdersByDeliveryDateAndStatus(Row):
    delivery_date: date | None
    status_code: str
    open_orders: int


class TopDeliveryDateByOpenOrders(Row):
    delivery_date: date | None
    open_orders: int


class PendingItemsByProduct(Row):
    product_id: int | None
    pending_items: int
    pending_quantity: int | None


class TopCustomerByPendingOrders(Row):
    customer_id: int | None
    customer_name: str | None
    pending_orders: int


class EntityFreshness(Row):
    entity: str
    last_changed_at: datetime | None
    lag_seconds: float | None


class Freshness(Row):
    checked_at: datetime
    entities: list[EntityFreshness]


class Health(Row):
    status: Literal["ok", "unavailable"]
    database: Literal["reachable", "unreachable"]


class ErrorDetail(Row):
    detail: str
