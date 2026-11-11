import pytest

from tests.infra.gold_fixtures import PUBLIC_VIEWS
from tests.infra.gold_reader_setup import make_db_logins
from tests.infra.grafana_setup import (
    as_grafana_reader,
    break_grafana_reader,
    request,
    start_grafana,
)
from tests.infra.stack import Stack


@pytest.fixture(scope="module")
def grafana(stack: Stack) -> Stack:
    start_grafana(stack)
    return stack


def test_grafana_is_healthy(grafana: Stack) -> None:
    health = request(grafana, "/api/health", admin=False)

    assert isinstance(health, dict)
    assert health["database"] == "ok"


def test_the_gold_datasource_is_provisioned_read_only(grafana: Stack) -> None:
    datasource = request(grafana, "/api/datasources/uid/gold")

    assert isinstance(datasource, dict)
    assert datasource["type"] == "grafana-postgresql-datasource"
    assert datasource["readOnly"] is True
    assert datasource["isDefault"] is True


def test_the_gold_datasource_connects(grafana: Stack) -> None:
    health = request(grafana, "/api/datasources/uid/gold/health")

    assert isinstance(health, dict)
    assert health["status"] == "OK"


def test_anonymous_visitors_can_browse_the_dashboards(grafana: Stack) -> None:
    assert isinstance(request(grafana, "/api/search", admin=False), list)


@pytest.mark.parametrize("view", sorted(PUBLIC_VIEWS))
def test_grafanas_login_reads_every_public_view(grafana: Stack, view: str) -> None:
    run = as_grafana_reader(grafana, f"SELECT count(*) FROM gold.{view};")

    assert run.returncode == 0, run.stderr


def test_grafanas_login_cannot_read_the_internal_tables(grafana: Stack) -> None:
    run = as_grafana_reader(grafana, "SELECT count(*) FROM gold_internal.current_orders;")

    assert run.returncode != 0
    assert "permission denied for schema gold_internal" in run.stderr


def test_grafanas_login_starts_read_only_with_a_thirty_second_timeout(grafana: Stack) -> None:
    run = as_grafana_reader(grafana, "SHOW default_transaction_read_only; SHOW statement_timeout;")

    assert run.returncode == 0, run.stderr
    assert run.stdout.split() == ["on", "30s"]


def test_make_db_logins_restores_grafanas_login(grafana: Stack) -> None:
    break_grafana_reader(grafana)

    restored = make_db_logins(grafana)
    health = request(grafana, "/api/datasources/uid/gold/health")

    assert restored.returncode == 0, restored.stderr
    assert isinstance(health, dict)
    assert health["status"] == "OK"
