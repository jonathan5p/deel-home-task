import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.report_setup import TIMED_OUT_ROUNDS, run_report
from pipelines.latency.html import render_index, render_run
from pipelines.latency.layers import Layer
from pipelines.latency.report import IndexEntry


def test_a_run_page_has_the_summary_percentiles_hops_and_probes() -> None:
    page = render_run(run_report(max_latency={Layer.GOLD: 60}))

    for heading in ("Summary", "Latency from the source commit", "Hop breakdown", "Probes"):
        assert f"<h2>{heading}" in page
    assert "Run of 2026-11-11T19:40:00.000Z, passed" in page
    assert "<dd>customers=7, orders=3</dd>" in page
    assert "<dd>gold ≤ 60 s</dd>" in page
    assert "<td>customers</td><td>gold</td>" in page
    assert '<td class="num">33.000</td>' in page
    assert "<td>kafka_to_landing</td>" in page
    assert "<svg" in page
    assert page.count('class="badge pass"') == 1 + 2


def test_a_failed_run_lists_its_timeouts_and_exceeded_thresholds() -> None:
    timed_out = render_run(run_report(TIMED_OUT_ROUNDS, max_latency={Layer.LANDING: 1}))

    assert ", failed</p>" in timed_out
    assert "<caption>Timeouts</caption>" in timed_out
    assert "<td>orders</td><td>none</td>" in timed_out
    assert "<caption>Thresholds exceeded</caption>" in timed_out
    assert "<dd>none</dd>" not in timed_out


def test_a_threshold_status_fails_for_an_entity_that_never_reached_the_layer() -> None:
    page = render_run(run_report(TIMED_OUT_ROUNDS, max_latency={Layer.GOLD: 60}))

    assert page.count('class="badge fail"') == 1 + 2


def test_a_run_without_thresholds_says_so() -> None:
    assert "<dt>Thresholds</dt><dd>none</dd>" in render_run(run_report())


def test_the_index_links_every_run_and_its_json() -> None:
    entries = [
        IndexEntry("20261111T090000Z", "2026-11-11T09:00:00.000Z", True, 5, ("customers",), 0, 0),
        IndexEntry("20261110T150405Z", "2026-11-10T15:04:05.000Z", False, 3, ("a", "b"), 1, 2),
    ]

    page = render_index(entries)

    assert page.index("20261111T090000Z.html") < page.index("20261110T150405Z.html")
    assert '<a href="20261110T150405Z.json">JSON</a>' in page
    assert "<td>a, b</td>" in page
    assert '<p class="subtitle">2 runs</p>' in page
