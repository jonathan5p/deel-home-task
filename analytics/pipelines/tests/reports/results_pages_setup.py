import json
from collections.abc import Mapping
from pathlib import Path

from pipelines.reports.results_pages import Committed

STATS = {"count": 2, "min": 9.5, "p50": 10.019, "p90": 11.021, "p95": 11.021, "max": 11.021}
GOLD = {"count": 2, "min": 38.0, "p50": 39.038, "p90": 41.03, "p95": 41.03, "max": 41.03}
HOP = {"count": 2, "min": 0.1, "p50": 0.214, "p90": 0.3, "p95": 0.3, "max": 0.3}

LATENCY_SUMMARY: dict[str, object] = {
    "passed": False,
    "probes": 2,
    "timeouts": [{"probe": 2, "entity": "orders", "last_layer": None}],
    "breaches": [
        {"probe": 1, "entity": "customers", "layer": "gold", "seconds": 61.5, "limit": 60}
    ],
    "latency_seconds": {
        "customers": {"landing": STATS, "gold": GOLD},
        "orders": {"landing": STATS, "gold": None},
    },
    "hop_seconds": {
        "customers": {"commit_to_debezium": HOP, "silver_to_gold": HOP},
        "orders": {"commit_to_debezium": HOP, "silver_to_gold": None},
    },
}

LATENCY_REPORT: dict[str, object] = {
    "run": {
        "started_at": "2026-11-11T22:33:45.647Z",
        "finished_at": "2026-11-11T22:36:01.201Z",
        "entities": ["customers", "orders"],
        "probes": 2,
        "poll_seconds": 1.0,
        "timeout_seconds": 300.0,
        "trigger_seconds": {"landing": 10, "gold": 10},
        "max_latency_seconds": {"gold": 60},
        "keys": {"customers": 1, "orders": 7},
    },
    "summary": LATENCY_SUMMARY,
    "probes": [
        {
            "probe": 1,
            "committed_at": "2026-11-11T22:33:51.432Z",
            "entities": {
                "customers": {"last_layer": "gold", "latency_seconds": {"gold": 61.5}},
                "orders": {"last_layer": "silver", "latency_seconds": {"silver": 30.033}},
            },
        },
        {
            "probe": 2,
            "committed_at": "2026-11-11T22:34:40.000Z",
            "entities": {"customers": {"last_layer": None, "latency_seconds": {}}},
        },
    ],
}

QUALITY_REPORT: dict[str, object] = {
    "checks": {
        "landing": {
            "results": [
                {
                    "check": "unique_offsets",
                    "subject": "topic-a/0",
                    "passed": True,
                    "metrics": {"rows": 10, "duplicated": 0},
                    "details": {"sample": [1]},
                    "description": "Every offset landed exactly once.",
                },
                {
                    "check": "unique_offsets",
                    "subject": "topic-a/1",
                    "passed": False,
                    "metrics": {"rows": 4, "duplicated": 1},
                    "details": {"sample_offsets": [3], "note": "a|b"},
                    "description": "Every offset landed exactly once.",
                },
            ],
            "summary": {"summary": True, "checks": 2, "failed": 1, "passed": False},
        },
        "gold": {
            "results": [
                {
                    "check": "gold_matches_silver",
                    "subject": "gold.orders",
                    "passed": True,
                    "metrics": {},
                    "description": "The gold view matches the lake view.",
                }
            ],
            "summary": {"summary": True, "checks": 1, "failed": 0, "passed": True},
        },
    },
    "summary": {"summary": True, "checks": 3, "failed": 1, "passed": False},
}

QUALITY_NAME = "20261110T151803Z.json"
LATENCY_NAME = "20261111T223345Z.json"


def committed(
    document: dict[str, object],
    report: str,
    *,
    caveat: str | None = None,
    skipped: tuple[tuple[str, str], ...] = (),
) -> Committed:
    return Committed(document=document, report=report, caveat=caveat, skipped=skipped)


def write_committed(
    folder: Path, document: object, selection: Mapping[str, object], *, page: bool = False
) -> None:
    folder.mkdir(parents=True)
    (folder / "latest.json").write_text(json.dumps(document))
    (folder / "selection.json").write_text(json.dumps(selection))
    if page:
        (folder / "latest.html").write_text("<html></html>")
