import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.latency_setup import (
    ENV,
    FakeClock,
    FakeLanding,
    FakeLookup,
    FakeSource,
    MemoryStore,
)
from analytics.pipelines.tests.latency.report_setup import TIMED_OUT_ROUNDS, run_report
from pipelines.latency.job import (
    ProbeIo,
    console_lines,
    main,
    round_line,
    run,
    summary_line,
    write_reports,
)
from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.report import ReportPaths
from pipelines.latency.settings import LatencySettings, ProbeOptions
from pipelines.settings import LakeSettings

SETTINGS = LatencySettings.from_env(ENV)
REPORTS = "s3a://bucket/latency/reports"
PATHS = ReportPaths(json=f"{REPORTS}/a.json", html=f"{REPORTS}/a.html")


def probe_io(*, gold: FakeLookup | None = None, source: FakeSource | None = None) -> ProbeIo:
    return ProbeIo(
        source=source or FakeSource(lowest={"customers": 1, "orders": 2}),
        landing=lambda topic: FakeLanding(),
        lookups={
            Layer.BRONZE: FakeLookup(),
            Layer.SILVER: FakeLookup(),
            Layer.GOLD: gold or FakeLookup(),
        },
        store=MemoryStore(),
    )


def options(*, probes: int = 2, max_latency: dict[Layer, float] | None = None) -> ProbeOptions:
    return ProbeOptions(
        entities=("customers", "orders"),
        probes=probes,
        timeout_seconds=3,
        max_latency=max_latency or {},
    )


def test_a_run_probes_every_entity_prints_the_tables_and_stores_the_reports() -> None:
    io = probe_io()
    lines: list[str] = []

    code = run(SETTINGS, options(), io, timing=FakeClock().timing(), out=lines.append)

    assert code == 0
    assert lines[0] == "LATENCY probing customers=1, orders=2: 2 probes"
    assert lines[1].startswith("LATENCY probe 1: customers landing=")
    summary = json.loads(lines[-1].removeprefix("LATENCY "))
    assert summary["passed"] is True
    assert summary["probes"] == 2
    assert summary["report"] == f"{REPORTS}/19700101T001640Z.json"
    store = io.store
    assert isinstance(store, MemoryStore)
    assert sorted(store.files) == [
        f"{REPORTS}/19700101T001640Z.html",
        f"{REPORTS}/19700101T001640Z.json",
        f"{REPORTS}/index.html",
    ]


def test_a_run_with_a_probe_that_never_reaches_gold_exits_1() -> None:
    lines: list[str] = []

    code = run(
        SETTINGS,
        options(probes=1),
        probe_io(gold=FakeLookup(after=None)),
        timing=FakeClock().timing(),
        out=lines.append,
    )

    assert code == 1
    assert "TIMEOUT probe 1 customers: last layer reached silver" in lines
    assert json.loads(lines[-1].removeprefix("LATENCY "))["timeouts"] == 2


def test_a_run_slower_than_a_threshold_exits_1() -> None:
    lines: list[str] = []

    code = run(
        SETTINGS,
        options(probes=1, max_latency={Layer.LANDING: 0.1}),
        probe_io(),
        timing=FakeClock().timing(),
        out=lines.append,
    )

    assert code == 1
    assert any(line.startswith("THRESHOLD probe 1 customers landing:") for line in lines)


def test_the_reports_are_written_with_an_index_of_every_run() -> None:
    store = MemoryStore()
    lake = LakeSettings.from_env(ENV)

    first = write_reports(store, lake, run_report(), datetime(2026, 11, 10, tzinfo=UTC))
    second = write_reports(
        store, lake, run_report(TIMED_OUT_ROUNDS), datetime(2026, 11, 11, tzinfo=UTC)
    )

    assert json.loads(store.files[first.json])["summary"]["passed"] is True
    assert store.files[second.html].startswith("<!doctype html>")
    index = store.files[f"{REPORTS}/index.html"]
    assert index.index("20261111T000000Z.html") < index.index("20261110T000000Z.html")


def test_the_console_shows_percentiles_per_layer_and_median_hops() -> None:
    lines = console_lines(run_report())

    assert lines[0] == "Latency in seconds from the source commit, nearest rank over 2 probes"
    assert lines[2].split() == ["customers", "landing", "2", "3.0", "3.0", "3.0", "3.0", "3.0"]
    assert "Median seconds per hop" in lines
    assert lines[-1].split() == ["orders", "0.1", "0.2", "2.7", "10.0", "10.0", "10.0"]


def test_the_console_marks_layers_never_reached() -> None:
    lines = console_lines(run_report(TIMED_OUT_ROUNDS))

    assert ["orders", "gold", "-", "-", "-", "-", "-", "-"] in [line.split() for line in lines]
    assert "TIMEOUT probe 1 orders: last layer reached none" in lines


def test_a_round_line_shows_each_entitys_latency_per_layer() -> None:
    (probe,) = TIMED_OUT_ROUNDS

    assert round_line(probe) == (
        "LATENCY probe 1: customers landing=3.0 bronze=13.0 silver=- gold=-;"
        " orders landing=- bronze=- silver=- gold=-"
    )


def test_the_summary_line_is_json_after_the_prefix() -> None:
    line = summary_line(run_report(), PATHS)

    assert line.startswith("LATENCY {")
    summary = json.loads(line.removeprefix("LATENCY "))
    assert summary["p95_seconds"]["customers"] == dict(
        zip(LAYERS, (3.0, 13.0, 23.0, 33.0), strict=True)
    )
    assert summary["html"] == PATHS.html


def test_missing_settings_fail_setup_with_exit_code_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([], {}) == 2
    assert "LATENCY setup failed: Missing required environment variables" in capsys.readouterr().err


def test_a_missing_duckdb_extension_fails_setup_with_exit_code_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = ENV | {"DUCKDB_EXTENSION_DIRECTORY": str(tmp_path)}

    assert main([], env) == 2
    assert "is not installed in" in capsys.readouterr().err


def test_invalid_options_exit_with_code_2() -> None:
    with pytest.raises(SystemExit) as error:
        main(["--probes", "0"], ENV)

    assert error.value.code == 2
