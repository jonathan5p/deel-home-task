import json
from collections.abc import Mapping, Sequence

FILER = "http://filer:8888/buckets/lake"
LATENCY_FOLDER = f"{FILER}/latency/reports"
QUALITY_FOLDER = f"{FILER}/quality/reports"


def latency_document(
    *, passed: bool = True, timeouts: int = 0, breaches: int = 0, started_at: str | None = None
) -> dict[str, object]:
    return {
        "run": {
            "started_at": started_at or "2026-11-11T22:33:45.647Z",
            "finished_at": "2026-11-11T22:36:01.201Z",
            "entities": ["customers", "orders"],
            "probes": 2,
            "poll_seconds": 1.0,
            "timeout_seconds": 300.0,
            "trigger_seconds": {"landing": 10, "bronze": 10, "silver": 10, "gold": 10},
            "max_latency_seconds": {},
            "marker_column": "updated_by",
            "keys": {"customers": 1, "orders": 1},
        },
        "summary": {
            "passed": passed,
            "probes": 2,
            "timeouts": [{"probe": 1, "entity": "orders", "last_layer": "silver"}] * timeouts,
            "breaches": [
                {"probe": 2, "entity": "customers", "layer": "gold", "seconds": 61.5, "limit": 60}
            ]
            * breaches,
            "latency_seconds": {},
            "hop_seconds": {},
        },
        "probes": [],
    }


def quality_document(
    checks: Sequence[str] = ("landing", "bronze", "silver", "gold"), *, passed: bool = True
) -> dict[str, object]:
    summary = {"summary": True, "checks": len(checks), "failed": 0 if passed else 1}
    return {
        "checks": {name: {"results": [], "summary": summary} for name in checks},
        "summary": summary | {"passed": passed},
    }


def encoded(document: object) -> bytes:
    return (json.dumps(document, indent=2) + "\n").encode()


def listing(folder: str, names: Sequence[str], *, more: bool = False, last: str = "") -> bytes:
    """A SeaweedFS filer directory listing, as it answers `Accept: application/json`."""
    path = folder.removeprefix("http://filer:8888")
    entries = [{"FullPath": f"{path}/{name}", "FileSize": 1} for name in names]
    return json.dumps(
        {
            "Path": path,
            "Entries": entries or None,
            "ShouldDisplayLoadMore": more,
            "LastFileName": last,
        }
    ).encode()


class FakeFiler:
    """Answers fetches from a fixed map of URLs; a missing URL is a 404."""

    def __init__(self, responses: Mapping[str, bytes]) -> None:
        self.responses = dict(responses)
        self.fetched: list[str] = []

    def __call__(self, url: str) -> bytes | None:
        self.fetched.append(url)
        return self.responses.get(url)


def filer_with(folder: str, reports: Mapping[str, bytes]) -> FakeFiler:
    """A filer whose `folder` holds `reports` by file name, listed on one page."""
    files = {f"{folder}/{name}": content for name, content in reports.items()}
    return FakeFiler({f"{folder}/?limit=1000": listing(folder, list(reports)), **files})
