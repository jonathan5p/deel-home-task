import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = REPO_ROOT / "monitoring/grafana/dashboards"
PROVIDERS = REPO_ROOT / "monitoring/grafana/provisioning/dashboards"
MOUNTED_DASHBOARDS = Path("/etc/grafana/dashboards")
QUERIES = REPO_ROOT / "analytics/sql/gold"
GOLD_FOLDER = "Gold"
SPARK_AND_LAKE_FOLDER = "Spark and lake"
LINK_TAG = "spark-and-lake"
GOLD = "gold-metrics"
GOLD_DATASOURCE = "gold"
PROMETHEUS_DATASOURCE = "prometheus"
PANEL_QUERIES = {
    "Open orders by delivery date and status": "01_open_orders_by_delivery_date_and_status.sql",
    "Top 3 delivery dates by open orders": "02_top_delivery_dates_by_open_orders.sql",
    "Pending items by product": "03_pending_items_by_product.sql",
    "Top 3 customers by pending orders": "04_top_customers_by_pending_orders.sql",
}
FRESHNESS_PANEL = "Freshness: time since the newest change"
RELATION = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", re.IGNORECASE)
LABEL_VALUES = re.compile(r"^label_values\((?P<selector>.+),\s*\w+\)$")

# Grafana's dashboard model and provisioning files are untyped, so they stay plain dicts.
type Panel = dict[str, Any]


def dashboard_files() -> list[Path]:
    return sorted(DASHBOARDS.rglob("*.json"))


def providers() -> list[dict[str, Any]]:
    return [
        provider
        for path in sorted(PROVIDERS.glob("*.yaml"))
        for provider in yaml.safe_load(path.read_text(encoding="utf-8"))["providers"]
    ]


def provided_files(provider: dict[str, Any]) -> list[Path]:
    """The dashboard files a provider loads: its path's JSON files, subdirectories included, as
    Grafana walks them."""
    path = DASHBOARDS / Path(provider["options"]["path"]).relative_to(MOUNTED_DASHBOARDS)
    return [path] if path.is_file() else sorted(path.rglob("*.json"))


def folder_dashboards(folder: str) -> list[str]:
    """The uids of the dashboards provisioned into a Grafana folder."""
    return sorted(
        path.stem
        for provider in providers()
        if provider["folder"] == folder
        for path in provided_files(provider)
    )


def dashboard(uid: str) -> dict[str, Any]:
    [path] = [path for path in dashboard_files() if path.stem == uid]
    return json.loads(path.read_text(encoding="utf-8"))


def panels(uid: str) -> list[Panel]:
    """The panels that show data, without the rows that group them."""
    return [panel for panel in dashboard(uid)["panels"] if panel["type"] != "row"]


def targets(uid: str) -> Iterator[tuple[Panel, dict[str, Any]]]:
    for panel in panels(uid):
        for target in panel["targets"]:
            yield panel, target


def panel(uid: str, title: str) -> Panel:
    [match] = [candidate for candidate in panels(uid) if candidate["title"] == title]
    return match


def variable_selectors(uid: str) -> dict[str, str]:
    """The series selector of each `label_values` template variable."""
    selectors = {}
    for variable in dashboard(uid)["templating"]["list"]:
        found = LABEL_VALUES.match(variable["definition"])
        assert found, variable["name"]
        selectors[variable["name"]] = found["selector"]
    return selectors


def relations(sql: str) -> set[str]:
    """The schema-qualified relations a query reads."""
    return {name.lower() for name in RELATION.findall(sql)}


def question_query(file: str) -> str:
    return (QUERIES / file).read_text(encoding="utf-8").strip()
