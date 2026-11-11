import copy
import json
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from pipelines.observability.stage_exporter import Json, epoch_seconds

HISTORY = Path(__file__).parent / "history"
APP_ID = "app-20261111200737-0000"
APP_PATH = f"applications/{APP_ID}"
APP_NAME = "SparkUICheck"
QUERY = "rate_window_counts"
SHUFFLE_WRITE_STAGE = 22
SHUFFLE_READ_STAGE = 23
SUMMARY_QUANTILES = "0,0.5,0.95,1"


def load(name: str) -> Json:
    return json.loads((HISTORY / f"{name}.json").read_text(encoding="utf-8"))


def applications() -> Json:
    """The History Server's application list, captured from a Spark 4.1.4 run of
    tests/infra/apps/spark_ui_check.py."""
    return load("applications")


def stages() -> Json:
    return load("stages")


def stage(stage_id: int) -> Json:
    [found] = [item for item in stages() if item["stageId"] == stage_id]
    return found


def summary(stage_id: int) -> Json:
    return load(f"task_summary_{stage_id}")


def summary_path(stage_id: int) -> str:
    return f"{APP_PATH}/stages/{stage_id}/0/taskSummary?quantiles={SUMMARY_QUANTILES}"


def running(listing: Json) -> Json:
    """The listing as it looks while the application still runs."""
    changed = copy.deepcopy(listing)
    for attempt in changed[0]["attempts"]:
        attempt.update(completed=False, endTimeEpoch=-1)
    return changed


def with_quantiles(summary_json: Json, metric: str, field_name: str, values: list[float]) -> Json:
    changed = copy.deepcopy(summary_json)
    changed[metric][field_name] = values
    return changed


def before_first_stage() -> float:
    return min(epoch_seconds(item["completionTime"]) for item in stages()) - 1


def between_the_stages() -> float:
    """A start time after the shuffle write stage completed and before the read stage did."""
    return epoch_seconds(stage(SHUFFLE_READ_STAGE)["completionTime"]) - 0.001


@dataclass
class FakeHistoryServer:
    """Answers REST paths from fixed responses and remembers what was asked; a path in
    `failing_once` fails the first time it is asked."""

    responses: dict[str, Json]
    calls: list[str] = field(default_factory=list)
    failing_once: set[str] = field(default_factory=set)

    @classmethod
    def captured(cls, listing: Json | None = None) -> FakeHistoryServer:
        responses: dict[str, Json] = {
            "applications": applications() if listing is None else listing,
            f"{APP_PATH}/stages?status=complete": stages(),
        }
        for stage_id in (SHUFFLE_WRITE_STAGE, SHUFFLE_READ_STAGE):
            responses[summary_path(stage_id)] = summary(stage_id)
        return cls(responses)

    def __call__(self, path: str) -> Json:
        self.calls.append(path)
        if path in self.failing_once:
            self.failing_once.discard(path)
            raise OSError(f"{path} is unavailable")
        return self.responses[path]


def sample(registry_value: float | None) -> float:
    assert registry_value is not None
    return registry_value


def labels(query: str = QUERY) -> dict[str, str]:
    return {"app": APP_NAME, "query": query}


class EchoPath(BaseHTTPRequestHandler):
    """Answers every GET with the requested path as JSON."""

    def do_GET(self) -> None:
        body = json.dumps({"path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass
