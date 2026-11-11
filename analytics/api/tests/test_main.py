from fastapi.testclient import TestClient
from psycopg.rows import dict_row

from analytics.api.tests.main_setup import DATABASE, PoolFactory
from api.main import create_app_from_settings, create_pool
from api.settings import ApiSettings


def test_the_pool_connects_as_the_configured_user_and_starts_closed() -> None:
    settings = ApiSettings.from_env(DATABASE | {"API_DB_POOL_MAX_SIZE": "7"})

    pool = create_pool(settings)

    assert pool.conninfo == "postgresql://db:5432/gold"
    assert pool.kwargs == {
        "user": "reader",
        "password": "secret",
        "autocommit": True,
        "row_factory": dict_row,
    }
    assert (pool.min_size, pool.max_size, pool.timeout) == (1, 7, 5.0)
    assert pool.closed


def test_the_app_opens_the_pool_without_waiting_and_closes_it_on_shutdown() -> None:
    factory = PoolFactory()
    settings = ApiSettings.from_env(DATABASE)

    with TestClient(create_app_from_settings(settings, factory)) as client:
        response = client.get("/health")
        events_while_serving = list(factory.pool.events)

    assert factory.settings == [settings]
    assert response.status_code == 200
    assert factory.pool.queries.queries == ["SELECT 1 AS reachable"]
    assert events_while_serving == ["open wait=False"]
    assert factory.pool.events == ["open wait=False", "close"]
