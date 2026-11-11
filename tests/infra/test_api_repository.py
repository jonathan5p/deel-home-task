from collections.abc import Iterator

import pytest

from tests.infra.api_setup import add_current_state, api_answers, remove_current_state
from tests.infra.stack import Stack

QUERY_FILES = (
    "01_open_orders_by_delivery_date_and_status.sql",
    "02_top_delivery_dates_by_open_orders.sql",
    "03_pending_items_by_product.sql",
    "04_top_customers_by_pending_orders.sql",
)


@pytest.fixture(scope="module")
def answers(stack: Stack) -> Iterator[dict[str, dict[str, list[object]]]]:
    add_current_state(stack)
    try:
        yield api_answers(stack)
    finally:
        remove_current_state(stack)


@pytest.mark.parametrize("query_file", QUERY_FILES)
def test_each_metric_returns_the_rows_of_its_business_query(
    answers: dict[str, dict[str, list[object]]], query_file: str
) -> None:
    answer = answers[query_file]

    assert answer["api"]
    assert answer["api"] == answer["sql"]


def test_freshness_reports_the_freshness_view_with_a_lag_for_every_entity_with_data(
    answers: dict[str, dict[str, list[object]]],
) -> None:
    freshness = answers["freshness"]

    entities = [entity[0] for entity in freshness["api"] if isinstance(entity, list)]

    assert entities == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]
    assert freshness["api"] == freshness["sql"]
