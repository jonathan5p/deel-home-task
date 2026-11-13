import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from tests.infra.gold_fixtures import CURRENT_STATE, REMOVE_CURRENT_STATE, rows
from tests.infra.grafana_setup import Json, request
from tests.infra.stack import REPO_ROOT, Stack

DASHBOARD = REPO_ROOT / "monitoring/grafana/dashboards/gold-metrics.json"

type Cell = str | Decimal | None


def dashboard_panels() -> list[Json]:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))["panels"]


def panel_sql(title: str) -> str:
    [panel] = [panel for panel in dashboard_panels() if panel["title"] == title]
    [target] = panel["targets"]
    return target["rawSql"]


def add_current_state(stack: Stack) -> None:
    rows(stack, REMOVE_CURRENT_STATE + CURRENT_STATE)


def remove_current_state(stack: Stack) -> None:
    rows(stack, REMOVE_CURRENT_STATE)


def cell(value: str) -> Cell:
    """A psql value as the type Grafana returns it: a number, a string, or null for empty."""
    if value == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return value


def frame_cell(kind: str, value: object) -> Cell:
    """A Grafana frame value in the same terms: a date is epoch milliseconds in UTC."""
    if value is None:
        return None
    if kind == "time":
        return datetime.fromtimestamp(int(str(value)) / 1000, UTC).date().isoformat()
    if kind == "number":
        return Decimal(str(value))
    return str(value)


def through_grafana(stack: Stack, sql: str) -> list[list[Cell]]:
    """Run a query through the gold datasource, as an anonymous viewer, and return its rows."""
    body: Json = {
        "from": "now-1h",
        "to": "now",
        "queries": [
            {"refId": "A", "datasource": {"uid": "gold"}, "format": "table", "rawSql": sql}
        ],
    }
    response = request(stack, "/api/ds/query", body=body, admin=False)
    assert isinstance(response, dict)
    [frame] = response["results"]["A"]["frames"]
    kinds = [field["type"] for field in frame["schema"]["fields"]]
    columns = frame["data"]["values"]
    return [
        [frame_cell(kind, value) for kind, value in zip(kinds, row, strict=True)]
        for row in zip(*columns, strict=True)
    ]


def through_psql(stack: Stack, sql: str) -> list[list[Cell]]:
    return [[cell(value) for value in row] for row in rows(stack, sql)]
