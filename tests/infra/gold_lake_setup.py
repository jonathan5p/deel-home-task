from dataclasses import dataclass
from typing import NamedTuple, Self

from tests.infra.gold_facts_setup import BATCHES
from tests.infra.gold_silver_setup import Json, SilverFixture
from tests.infra.stack import Stack

LAKE_SILVER = SilverFixture("test-fixtures/gold-lake", "gold_lake_fixture", BATCHES)
GOLD_LAKE_SQL = "tests/infra/sql/gold_views.sql"
COMPARED_VIEWS = (
    "dim_date",
    "dim_order_status",
    "dim_customer",
    "dim_product",
    "fact_order_history",
    "fact_order_item_history",
    "current_orders",
    "current_order_items",
    "open_orders",
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)


class Matching(NamedTuple):
    rows: int
    lake_only: int
    postgres_only: int

    @classmethod
    def of(cls, result: Json) -> Self:
        return cls(*(int(str(result[field])) for field in cls._fields))


@dataclass(frozen=True, slots=True)
class LakeComparison:
    refreshes: Json
    views: dict[str, Matching]


def run_lake_scenario(stack: Stack) -> LakeComparison:
    for batch in BATCHES:
        LAKE_SILVER.run_gold(stack, batch)
    refreshes, *views = LAKE_SILVER.compare_lake_views(stack)
    matching = {str(view["view"]): Matching.of(view) for view in views}
    return LakeComparison(refreshes=refreshes, views=matching)
