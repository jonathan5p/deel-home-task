from collections.abc import Iterator
from decimal import Decimal

import pytest

from tests.infra.grafana_dashboard_setup import (
    add_current_state,
    dashboard_panels,
    panel_sql,
    remove_current_state,
    through_grafana,
    through_psql,
)
from tests.infra.grafana_setup import request, start_grafana
from tests.infra.stack import Stack

QUESTIONS = (
    "Open orders by delivery date and status",
    "Top 3 delivery dates by open orders",
    "Pending items by product",
    "Top 3 customers by pending orders",
)
FRESHNESS = "Freshness: time since the newest change"


@pytest.fixture(scope="module")
def grafana(stack: Stack) -> Iterator[Stack]:
    start_grafana(stack)
    add_current_state(stack)
    try:
        yield stack
    finally:
        remove_current_state(stack)


def test_the_dashboard_is_provisioned_in_the_gold_folder(grafana: Stack) -> None:
    found = request(grafana, "/api/dashboards/uid/gold-metrics")

    assert isinstance(found, dict)
    assert found["meta"]["folderTitle"] == "Gold"
    assert found["meta"]["provisioned"] is True
    assert len(found["dashboard"]["panels"]) == len(dashboard_panels())


def test_anonymous_visitors_land_on_the_dashboard(grafana: Stack) -> None:
    home = request(grafana, "/api/dashboards/home", admin=False)

    assert isinstance(home, dict)
    assert home["dashboard"]["uid"] == "gold-metrics"


@pytest.mark.parametrize("title", QUESTIONS)
def test_a_question_panel_shows_the_rows_of_its_gold_view(grafana: Stack, title: str) -> None:
    sql = panel_sql(title)
    shown = through_grafana(grafana, sql)

    assert shown
    assert shown == through_psql(grafana, sql)


def test_the_freshness_panel_shows_one_lag_per_entity(grafana: Stack) -> None:
    shown = through_grafana(grafana, panel_sql(FRESHNESS))

    assert [entity for entity, _ in shown] == ["customers", "order_items", "orders", "products"]
    assert all(lag is None or (isinstance(lag, Decimal) and lag >= 0) for _, lag in shown)
