from collections.abc import Iterator

import pytest

from tests.infra.api_service_setup import as_psql_rows, get, get_json, health_status, start_api
from tests.infra.api_setup import add_current_state, remove_current_state
from tests.infra.gold_fixtures import GOLD_QUERIES, rows
from tests.infra.stack import Stack, wait_until

METRICS = {
    "/metrics/open-orders-by-delivery-date-and-status": (
        "01_open_orders_by_delivery_date_and_status.sql"
    ),
    "/metrics/top-delivery-dates-by-open-orders": "02_top_delivery_dates_by_open_orders.sql",
    "/metrics/pending-items-by-product": "03_pending_items_by_product.sql",
    "/metrics/top-customers-by-pending-orders": "04_top_customers_by_pending_orders.sql",
}


@pytest.fixture(scope="module")
def api(stack: Stack) -> Iterator[Stack]:
    start_api(stack)
    add_current_state(stack)
    try:
        yield stack
    finally:
        remove_current_state(stack)


def test_the_api_service_starts_healthy(api: Stack) -> None:
    assert health_status(api) == "healthy"


def test_health_reports_the_database_reachable(api: Stack) -> None:
    assert get_json(api, "/health") == (200, {"status": "ok", "database": "reachable"})


@pytest.mark.parametrize(("path", "query_file"), METRICS.items())
def test_each_metric_endpoint_returns_the_rows_of_its_business_query(
    api: Stack, path: str, query_file: str
) -> None:
    status, body = get_json(api, path)

    assert status == 200
    assert as_psql_rows(body) == rows(api, (GOLD_QUERIES / query_file).read_text())
    assert body


def test_freshness_lists_every_entity(api: Stack) -> None:
    status, body = get_json(api, "/freshness")

    assert status == 200
    assert isinstance(body, dict)
    assert [entity["entity"] for entity in body["entities"]] == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]


def test_the_openapi_document_and_the_interactive_docs_are_served(api: Stack) -> None:
    status, document = get_json(api, "/openapi.json")
    docs_status, docs = get(api, "/docs")

    assert status == 200
    assert isinstance(document, dict)
    assert set(document["paths"]) == {*METRICS, "/health", "/freshness"}
    assert docs_status == 200
    assert "swagger" in docs.lower()


def test_health_is_unavailable_while_the_database_is_down_and_recovers(api: Stack) -> None:
    api.compose("stop", "analytics-db")
    try:
        down = get_json(api, "/health")
    finally:
        api.compose("up", "--detach", "--wait", "analytics-db")

    assert down == (503, {"status": "unavailable", "database": "unreachable"})
    wait_until(
        lambda: get(api, "/health")[0] == 200,
        timeout=120,
        message="The API never reconnected to the analytics database",
    )
