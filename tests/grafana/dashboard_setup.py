import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = REPO_ROOT / "monitoring/grafana/dashboards/gold-metrics.json"
QUERIES = REPO_ROOT / "analytics/sql/gold"
DATASOURCE_UID = "gold"
PANEL_QUERIES = {
    "Open orders by delivery date and status": "01_open_orders_by_delivery_date_and_status.sql",
    "Top 3 delivery dates by open orders": "02_top_delivery_dates_by_open_orders.sql",
    "Pending items by product": "03_pending_items_by_product.sql",
    "Top 3 customers by pending orders": "04_top_customers_by_pending_orders.sql",
}
FRESHNESS_PANEL = "Freshness: time since the newest change"
RELATION = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", re.IGNORECASE)

# Grafana's dashboard model is untyped JSON, so the panels stay plain dicts.
type Panel = dict[str, Any]


def dashboard() -> dict[str, Any]:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def panels() -> list[Panel]:
    return dashboard()["panels"]


def targets() -> Iterator[tuple[Panel, dict[str, Any]]]:
    for panel in panels():
        for target in panel["targets"]:
            yield panel, target


def panel(title: str) -> Panel:
    [match] = [candidate for candidate in panels() if candidate["title"] == title]
    return match


def relations(sql: str) -> set[str]:
    """The schema-qualified relations a query reads."""
    return {name.lower() for name in RELATION.findall(sql)}


def question_query(file: str) -> str:
    return (QUERIES / file).read_text(encoding="utf-8").strip()
