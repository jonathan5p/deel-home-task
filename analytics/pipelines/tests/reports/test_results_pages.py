from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from analytics.pipelines.tests.reports.results_pages_setup import (
    LATENCY_NAME,
    LATENCY_REPORT,
    LATENCY_SUMMARY,
    QUALITY_NAME,
    QUALITY_REPORT,
    committed,
    write_committed,
)
from pipelines.reports.latest import REPORT_NAME
from pipelines.reports.results_pages import (
    LATENCY_PAGE,
    QUALITY_PAGE,
    Committed,
    latency_calculated_at,
    latency_page,
    markdown_table,
    quality_calculated_at,
    quality_page,
    read_committed,
    results_pages,
    utc_text,
)


def test_times_are_written_in_utc() -> None:
    assert utc_text(datetime(2026, 11, 11, 22, 33, 45, 647000, tzinfo=UTC)) == (
        "2026-11-11 22:33:45 UTC"
    )


def test_a_latency_run_was_calculated_when_it_started() -> None:
    assert latency_calculated_at(LATENCY_REPORT) == "2026-11-11 22:33:45 UTC"


def test_a_quality_run_was_calculated_at_the_time_its_report_is_named_after() -> None:
    assert REPORT_NAME.fullmatch(QUALITY_NAME)
    assert quality_calculated_at(QUALITY_NAME) == "2026-11-10 15:18:03 UTC"


def test_a_table_escapes_pipes_and_line_breaks_in_its_cells() -> None:
    assert markdown_table(["a", "b"], [["x|y", "one\ntwo"]]) == [
        "| a | b |",
        "| --- | --- |",
        "| x\\|y | one two |",
    ]


def test_the_latency_page_is_stamped_with_the_run_and_the_report() -> None:
    page = latency_page(committed(LATENCY_REPORT, LATENCY_NAME))

    assert page.startswith("# Latest latency results\n")
    assert (
        '!!! info "Calculated on 2026-11-11 22:33:45 UTC from report `20261111T223345Z.json`"'
    ) in page
    assert "it finished on 2026-11-11 22:36:01 UTC." in page
    assert "[full HTML report](../results/latency/latest.html)" in page
    assert "[JSON report](../results/latency/latest.json)" in page


def test_the_latency_page_summarises_the_run_and_its_settings() -> None:
    page = latency_page(committed(LATENCY_REPORT, LATENCY_NAME))

    for row in [
        "| Result | **Failed** |",
        "| Probes | 2 |",
        "| Entities | customers, orders |",
        "| Timeouts | 1 |",
        "| Limits exceeded | 1 |",
        "| Trigger intervals | landing=10 s, gold=10 s |",
        "| Limits | gold=60 s |",
        "| Poll interval | 1.0 s |",
        "| Probe timeout | 300.0 s |",
        "| Probed keys | customers=1, orders=7 |",
    ]:
        assert row in page


def test_the_latency_page_has_the_percentiles_per_entity_and_layer_and_the_hop_medians() -> None:
    page = latency_page(committed(LATENCY_REPORT, LATENCY_NAME))

    assert "| Entity | landing | gold |" in page
    assert "| customers | 10.02 / 11.02 | 39.04 / 41.03 |" in page
    assert "| orders | 10.02 / 11.02 | not reached |" in page
    assert "| Entity | `commit_to_debezium` | `silver_to_gold` |" in page
    assert "| customers | 0.21 | 0.21 |" in page
    assert "| orders | 0.21 | not reached |" in page


def test_the_latency_page_lists_every_probe_timeout_and_exceeded_limit() -> None:
    page = latency_page(committed(LATENCY_REPORT, LATENCY_NAME))

    assert "| Probe | Committed at | customers | orders |" in page
    assert "| 1 | 2026-11-11T22:33:51.432Z | 61.50 s at gold | 30.03 s at silver |" in page
    assert "| 2 | 2026-11-11T22:34:40.000Z | not reached | not reached |" in page
    assert "## Timeouts" in page
    assert "| 2 | orders | none |" in page
    assert "## Limits exceeded" in page
    assert "| 1 | customers | gold | 61.5 | 60 |" in page


def test_a_passing_latency_run_has_no_timeout_or_limit_sections() -> None:
    summary = LATENCY_SUMMARY | {"passed": True, "timeouts": [], "breaches": []}
    page = latency_page(committed(LATENCY_REPORT | {"summary": summary}, LATENCY_NAME))

    assert "| Result | Passed |" in page
    assert "## Timeouts" not in page
    assert "## Limits exceeded" not in page
    assert "!!! failure" not in page
    assert "!!! warning" not in page


def test_the_page_says_why_newer_reports_are_not_shown() -> None:
    page = latency_page(
        committed(
            LATENCY_REPORT,
            LATENCY_NAME,
            skipped=(("20261112T010000Z.json", "did not pass: 1 timeout, 0 limits exceeded"),),
        )
    )

    assert '!!! warning "Newer reports not shown"' in page
    assert "    - `20261112T010000Z.json` did not pass: 1 timeout, 0 limits exceeded" in page


def test_the_page_says_when_the_shown_report_has_a_problem_of_its_own() -> None:
    page = latency_page(
        committed(LATENCY_REPORT, LATENCY_NAME, caveat="did not pass: 1 timeout, 1 limit exceeded")
    )

    assert '!!! failure "No newer report was complete and passing"' in page
    assert "This report did not pass: 1 timeout, 1 limit exceeded; it is shown as it is." in page


@pytest.mark.parametrize(
    ("render", "make_target"), [(latency_page, "latency-probe"), (quality_page, "data-quality")]
)
def test_without_a_committed_report_the_page_says_how_to_make_one(
    render: Callable[[Committed | None], str], make_target: str
) -> None:
    page = render(None)

    assert '!!! warning "No report committed yet"' in page
    assert f"Run `make {make_target}` on the running stack, then `make docs-results`" in page


def test_the_quality_page_is_stamped_and_summarises_the_run_and_each_check() -> None:
    page = quality_page(committed(QUALITY_REPORT, QUALITY_NAME))

    assert page.startswith("# Latest data quality results\n")
    assert (
        '!!! info "Calculated on 2026-11-10 15:18:03 UTC from report `20261110T151803Z.json`"'
    ) in page
    assert "of the checks `landing`, `gold`." in page
    assert "[JSON report](../results/quality/latest.json)" in page
    assert "**Failed**: 3 results, 1 failed." in page
    assert "| [`landing`](#landing) | **Failed** | 2 | 1 |" in page
    assert "| [`gold`](#gold) | Passed | 1 | 0 |" in page


def test_the_quality_page_lists_every_result_with_its_metrics() -> None:
    page = quality_page(committed(QUALITY_REPORT, QUALITY_NAME))

    assert "## landing\n\n**Failed**: 2 results, 1 failed." in page
    assert "| `unique_offsets` | topic-a/0 | Passed | rows=10, duplicated=0 |" in page
    assert "| `unique_offsets` | topic-a/1 | **Failed** | rows=4, duplicated=1 |" in page
    assert "| `gold_matches_silver` | gold.orders | Passed | none |" in page
    assert "| unique_offsets | Every offset landed exactly once. |" in page


def test_the_quality_page_shows_the_details_of_failed_results_only() -> None:
    page = quality_page(committed(QUALITY_REPORT, QUALITY_NAME))

    assert '??? failure "Details of `unique_offsets` on topic-a/1"' in page
    assert '      "note": "a|b",' in page
    assert '"sample": [' not in page


def test_reading_the_committed_report_and_its_selection(tmp_path: Path) -> None:
    selection = {
        "report": LATENCY_NAME,
        "caveat": "did not pass",
        "skipped": [{"report": "20261112T010000Z.json", "reason": "is not a latency report"}],
    }
    write_committed(tmp_path / "latency", LATENCY_REPORT, selection)

    assert read_committed(tmp_path / "latency") == Committed(
        document=LATENCY_REPORT,
        report=LATENCY_NAME,
        caveat="did not pass",
        skipped=(("20261112T010000Z.json", "is not a latency report"),),
    )


def test_a_folder_without_a_report_or_selection_has_nothing_committed(tmp_path: Path) -> None:
    write_committed(tmp_path / "quality", QUALITY_REPORT, {"report": QUALITY_NAME})
    (tmp_path / "quality" / "selection.json").unlink()

    assert read_committed(tmp_path / "quality") is None
    assert read_committed(tmp_path / "latency") is None


def test_the_results_pages_render_what_is_committed(tmp_path: Path) -> None:
    write_committed(tmp_path / "latency", LATENCY_REPORT, {"report": LATENCY_NAME}, page=True)

    pages = results_pages(tmp_path)

    assert set(pages) == {LATENCY_PAGE, QUALITY_PAGE}
    assert "from report `20261111T223345Z.json`" in pages[LATENCY_PAGE]
    assert "No report committed yet" in pages[QUALITY_PAGE]
