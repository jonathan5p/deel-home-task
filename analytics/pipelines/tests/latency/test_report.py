import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.latency_setup import MemoryStore
from analytics.pipelines.tests.latency.report_setup import (
    TIMED_OUT_ROUNDS,
    run_report,
)
from pipelines.latency.layers import Layer
from pipelines.latency.report import (
    IndexEntry,
    index_entries,
    index_entry,
    index_path,
    iso_time,
    report_document,
    report_paths,
)
from pipelines.settings import LakeSettings

LAKE = LakeSettings.from_env({"LAKE_ROOT": "s3a://lake/", "ICEBERG_CATALOG": "lake"})
NOW = datetime(2026, 11, 10, 15, 4, 5, tzinfo=UTC)


def test_times_are_utc_iso_strings_with_milliseconds() -> None:
    assert iso_time(1_794_426_435_489) == "2026-11-11T19:47:15.489Z"


def test_the_document_holds_the_runs_settings_and_targets() -> None:
    run = report_document(run_report(max_latency={Layer.GOLD: 60}))["run"]

    assert run == {
        "started_at": "2026-11-11T19:40:00.000Z",
        "finished_at": "2026-11-11T19:41:30.000Z",
        "entities": ["customers", "orders"],
        "probes": 2,
        "poll_seconds": 1.0,
        "timeout_seconds": 300.0,
        "trigger_seconds": {"landing": 10, "bronze": 10, "silver": 10, "gold": 10},
        "max_latency_seconds": {"gold": 60},
        "marker_column": "updated_by",
        "keys": {"customers": 7, "orders": 3},
    }


def test_the_summary_has_the_percentiles_by_entity_and_layer_and_the_hops() -> None:
    summary = report_document(run_report())["summary"]

    assert isinstance(summary, dict)
    assert summary["passed"] is True
    assert summary["probes"] == 2
    assert summary["latency_seconds"]["orders"]["gold"] == {
        "count": 2,
        "min": 33.0,
        "p50": 33.0,
        "p90": 33.0,
        "p95": 33.0,
        "max": 33.0,
    }
    assert summary["hop_seconds"]["customers"]["kafka_to_landing"]["p50"] == 2.7


def test_a_timeout_or_an_exceeded_threshold_fails_the_run() -> None:
    timed_out = report_document(run_report(TIMED_OUT_ROUNDS))["summary"]
    too_slow = report_document(run_report(max_latency={Layer.SILVER: 20}))["summary"]

    assert isinstance(timed_out, dict)
    assert isinstance(too_slow, dict)
    assert timed_out["passed"] is False
    assert timed_out["timeouts"] == [
        {"probe": 1, "entity": "customers", "last_layer": "bronze"},
        {"probe": 1, "entity": "orders", "last_layer": None},
    ]
    assert timed_out["latency_seconds"]["orders"]["landing"] is None
    assert too_slow["passed"] is False
    assert len(too_slow["breaches"]) == 4


def test_each_probe_lists_every_entitys_change_and_when_each_layer_saw_it() -> None:
    probes = report_document(run_report(TIMED_OUT_ROUNDS))["probes"]

    assert isinstance(probes, list)
    (probe,) = probes
    assert probe["probe"] == 1
    assert probe["marker"] == -1
    assert probe["waited_seconds"] == 1.5
    customers, orders = probe["entities"]["customers"], probe["entities"]["orders"]
    assert customers["commit_sequence"] == 500
    assert customers["changed_at"] == "1970-01-01T00:16:40.000Z"
    assert customers["observed_at"]["bronze"] == "1970-01-01T00:16:53.000Z"
    assert customers["observed_at"]["gold"] is None
    assert customers["latency_seconds"] == {
        "landing": 3.0,
        "bronze": 13.0,
        "silver": None,
        "gold": None,
    }
    assert customers["last_layer"] == "bronze"
    assert orders["commit_sequence"] is None
    assert orders["last_layer"] is None


def test_the_document_is_plain_json() -> None:
    document = report_document(run_report(TIMED_OUT_ROUNDS))

    assert json.loads(json.dumps(document)) == document


def test_each_run_is_named_after_its_utc_time_next_to_the_index() -> None:
    bogota = NOW.astimezone(timezone(timedelta(hours=-5)))

    paths = report_paths(LAKE, bogota)

    assert paths.json == "s3a://lake/latency/reports/20261110T150405Z.json"
    assert paths.html == "s3a://lake/latency/reports/20261110T150405Z.html"
    assert index_path(LAKE) == "s3a://lake/latency/reports/index.html"


def test_an_index_entry_is_read_back_from_a_json_report() -> None:
    document = report_document(run_report(TIMED_OUT_ROUNDS))

    entry = index_entry("20261110T150405Z", json.loads(json.dumps(document)))

    assert entry == IndexEntry(
        name="20261110T150405Z",
        started_at="2026-11-11T19:40:00.000Z",
        passed=False,
        probes=1,
        entities=("customers", "orders"),
        timeouts=2,
        breaches=0,
    )
    assert entry.html == "20261110T150405Z.html"


@pytest.mark.parametrize(
    "document",
    [[], {"run": {}}, {"run": {"started_at": 1}, "summary": {}}, {"summary": {"passed": True}}],
)
def test_a_document_that_is_not_a_latency_report_has_no_index_entry(document: object) -> None:
    assert index_entry("x", document) is None


def test_the_index_lists_the_reports_newest_first_and_skips_unreadable_files() -> None:
    store = MemoryStore()
    folder = "s3a://lake/latency/reports"
    document = json.dumps(report_document(run_report()))
    store.files = {
        f"{folder}/20261110T150405Z.json": document,
        f"{folder}/20261111T090000Z.json": document,
        f"{folder}/20261109T000000Z.json": "not json",
        f"{folder}/20261108T000000Z.json": "{}",
        f"{folder}/20261111T090000Z.html": "<html>",
        f"{folder}/nested/20261112T000000Z.json": document,
    }

    entries = index_entries(store, LAKE)

    assert [entry.name for entry in entries] == ["20261111T090000Z", "20261110T150405Z"]
