import pytest

from analytics.api.tests.app_setup import METRIC_PATHS, STATUS_PATHS, client
from analytics.api.tests.fake_repository import FakeRepository
from api.repository import RepositoryUnavailableError


def test_open_orders_by_delivery_date_and_status_come_back_as_rows() -> None:
    response = client().get("/metrics/open-orders-by-delivery-date-and-status")

    assert response.status_code == 200
    assert response.json() == [
        {"delivery_date": "2026-11-10", "status_code": "PENDING", "open_orders": 2},
        {"delivery_date": None, "status_code": "UNKNOWN", "open_orders": 1},
    ]


def test_the_top_delivery_dates_keep_every_row_tied_for_third_place() -> None:
    response = client().get("/metrics/top-delivery-dates-by-open-orders")

    assert response.status_code == 200
    assert response.json() == [
        {"delivery_date": "2026-12-01", "open_orders": 3},
        {"delivery_date": "2026-12-02", "open_orders": 2},
        {"delivery_date": "2026-12-03", "open_orders": 1},
        {"delivery_date": "2026-12-04", "open_orders": 1},
    ]


def test_pending_items_by_product_keep_unknown_products_and_quantities() -> None:
    response = client().get("/metrics/pending-items-by-product")

    assert response.status_code == 200
    assert response.json() == [
        {"product_id": 10, "pending_items": 2, "pending_quantity": 10},
        {"product_id": None, "pending_items": 1, "pending_quantity": None},
    ]


def test_the_top_customers_keep_a_customer_without_a_name() -> None:
    response = client().get("/metrics/top-customers-by-pending-orders")

    assert response.status_code == 200
    assert response.json() == [
        {"customer_id": 1, "customer_name": "Ada Lovelace", "pending_orders": 3},
        {"customer_id": 4, "customer_name": None, "pending_orders": 1},
    ]


def test_a_metric_without_rows_is_an_empty_list() -> None:
    response = client(FakeRepository(pending_items=[])).get("/metrics/pending-items-by-product")

    assert response.status_code == 200
    assert response.json() == []


def test_health_reports_a_reachable_database() -> None:
    response = client().get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "reachable"}


def test_health_is_unavailable_while_the_database_is_unreachable() -> None:
    repository = FakeRepository(error=RepositoryUnavailableError("down"))

    response = client(repository).get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "unreachable"}


def test_freshness_reports_each_entity_with_its_lag_at_the_database_clock() -> None:
    response = client().get("/freshness")

    assert response.status_code == 200
    assert response.json() == {
        "checked_at": "2026-11-10T12:00:00Z",
        "entities": [
            {
                "entity": "orders",
                "last_changed_at": "2026-11-10T11:59:30Z",
                "lag_seconds": 30.0,
            },
            {"entity": "products", "last_changed_at": None, "lag_seconds": None},
        ],
    }


@pytest.mark.parametrize("path", [*METRIC_PATHS, "/freshness"])
def test_an_unavailable_database_answers_503_with_a_safe_message(path: str) -> None:
    repository = FakeRepository(error=RepositoryUnavailableError("The database is unavailable"))

    response = client(repository).get(path)

    assert response.status_code == 503
    assert response.json() == {"detail": "The database is unavailable"}


@pytest.mark.parametrize("path", [*METRIC_PATHS, "/freshness"])
def test_an_unexpected_error_answers_500_without_its_details(path: str) -> None:
    repository = FakeRepository(error=RuntimeError("secret internals"))

    response = client(repository).get(path)

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def test_the_openapi_document_lists_every_endpoint_with_its_response_schema() -> None:
    document = client().get("/openapi.json").json()

    assert set(document["paths"]) == {*METRIC_PATHS, *STATUS_PATHS}
    for path in document["paths"].values():
        responses = path["get"]["responses"]
        assert "schema" in responses["200"]["content"]["application/json"]
        assert "503" in responses


def test_the_interactive_documentation_is_served() -> None:
    response = client().get("/docs")

    assert response.status_code == 200
    assert "swagger" in response.text.lower()
