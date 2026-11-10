from fastapi.testclient import TestClient

from analytics.api.tests.fake_repository import FakeRepository
from api.app import create_app

METRIC_PATHS = (
    "/metrics/open-orders-by-delivery-date-and-status",
    "/metrics/top-delivery-dates-by-open-orders",
    "/metrics/pending-items-by-product",
    "/metrics/top-customers-by-pending-orders",
)
STATUS_PATHS = ("/health", "/freshness")


def client(repository: FakeRepository | None = None) -> TestClient:
    """A client of the app over the fake repository; server errors become 500 responses."""
    app = create_app(repository or FakeRepository())
    return TestClient(app, raise_server_exceptions=False)
