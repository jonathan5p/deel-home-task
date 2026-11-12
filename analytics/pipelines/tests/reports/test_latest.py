import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from analytics.pipelines.tests.reports.latest_setup import (
    FILER,
    LATENCY_FOLDER,
    QUALITY_FOLDER,
    FakeFiler,
    encoded,
    filer_with,
    latency_document,
    listing,
    quality_document,
)
from pipelines.reports.latest import (
    LATENCY,
    QUALITY,
    QUALITY_CHECKS,
    FilerUnavailableError,
    Skipped,
    http_fetch,
    latency_problem,
    list_names,
    main,
    newest_first,
    publish,
    quality_problem,
    select,
)

NEWEST = "20261111T223345Z.json"
OLDER = "20261111T220533Z.json"
OLDEST = "20261110T080000Z.json"


def test_only_report_names_count_and_the_newest_comes_first() -> None:
    names = [OLDER, "index.html", NEWEST, "20261111T223345Z.html", OLDEST, "notes.json"]

    assert newest_first(names) == [NEWEST, OLDER, OLDEST]


def test_a_latency_run_that_passed_has_no_problem() -> None:
    assert latency_problem(latency_document()) is None


def test_a_failed_latency_run_counts_its_timeouts_and_exceeded_limits() -> None:
    document = latency_document(passed=False, timeouts=2, breaches=1)

    assert latency_problem(document) == "did not pass: 2 timeouts, 1 limit exceeded"


@pytest.mark.parametrize(
    "document",
    [
        [],
        {"run": {}},
        {"run": {"started_at": "x"}, "summary": {"passed": "yes"}},
        quality_document(),
    ],
)
def test_a_document_that_is_not_a_latency_report_is_reported_as_such(document: object) -> None:
    assert latency_problem(document) == "is not a latency report"


def test_a_quality_run_of_every_check_has_no_problem_even_when_a_check_failed() -> None:
    assert quality_problem(quality_document(passed=False)) is None


def test_a_quality_run_of_some_checks_is_not_complete() -> None:
    assert quality_problem(quality_document(("landing",))) == (
        "is not a complete run: it lacks bronze, silver, gold"
    )


@pytest.mark.parametrize("document", [{}, {"checks": [], "summary": {}}, latency_document()])
def test_a_document_that_is_not_a_quality_report_is_reported_as_such(document: object) -> None:
    assert quality_problem(document) == "is not a data quality report"


def test_the_newest_acceptable_report_is_selected_and_the_newer_ones_are_named() -> None:
    reports = {
        NEWEST: encoded(latency_document(passed=False, timeouts=1)),
        OLDER: b"{not json",
        OLDEST: encoded(latency_document()),
    }

    selection = select(LATENCY, [NEWEST, OLDER, OLDEST], reports.get)

    assert selection is not None
    assert selection.report == OLDEST
    assert selection.content == reports[OLDEST]
    assert selection.caveat is None
    assert selection.skipped == (
        Skipped(NEWEST, "did not pass: 1 timeout, 0 limits exceeded"),
        Skipped(OLDER, "is not a latency report"),
    )


def test_without_an_acceptable_report_the_newest_one_is_selected_with_its_problem() -> None:
    reports = {
        NEWEST: b"[]",
        OLDER: encoded(latency_document(passed=False, timeouts=1)),
        OLDEST: encoded(latency_document(passed=False, timeouts=3)),
    }

    selection = select(LATENCY, [NEWEST, OLDER, OLDEST], reports.get)

    assert selection is not None
    assert selection.report == OLDER
    assert selection.caveat == "did not pass: 1 timeout, 0 limits exceeded"
    assert selection.skipped == (Skipped(NEWEST, "is not a latency report"),)


def test_no_report_of_the_kind_selects_nothing() -> None:
    reports = {NEWEST: b"[]", OLDER: None}

    assert select(QUALITY, [NEWEST, OLDER], reports.get) is None
    assert select(QUALITY, [], reports.get) is None


def test_the_selection_is_written_as_an_object() -> None:
    selection = select(
        QUALITY,
        [NEWEST, OLDER],
        {NEWEST: encoded(quality_document(("gold",))), OLDER: encoded(quality_document())}.get,
    )

    assert selection is not None
    assert selection.as_object() == {
        "report": OLDER,
        "caveat": None,
        "skipped": [
            {"report": NEWEST, "reason": "is not a complete run: it lacks landing, bronze, silver"}
        ],
    }


def test_the_listing_follows_every_page_of_the_filer() -> None:
    filer = FakeFiler(
        {
            f"{LATENCY_FOLDER}/?limit=1000": listing(
                LATENCY_FOLDER, [OLDEST, OLDER], more=True, last=OLDER
            ),
            f"{LATENCY_FOLDER}/?limit=1000&lastFileName={OLDER}": listing(LATENCY_FOLDER, [NEWEST]),
        }
    )

    assert list_names(filer, LATENCY_FOLDER) == [OLDEST, OLDER, NEWEST]


def test_a_missing_folder_lists_nothing() -> None:
    assert list_names(FakeFiler({}), QUALITY_FOLDER) == []


def test_an_empty_folder_lists_nothing() -> None:
    filer = FakeFiler({f"{QUALITY_FOLDER}/?limit=1000": listing(QUALITY_FOLDER, [])})

    assert list_names(filer, QUALITY_FOLDER) == []


def test_publishing_latency_copies_the_selected_report_and_its_page(tmp_path: Path) -> None:
    report = encoded(latency_document())
    filer = filer_with(
        LATENCY_FOLDER,
        {
            NEWEST: encoded(latency_document(passed=False, timeouts=1)),
            OLDER: report,
            "20261111T220533Z.html": b"<html>older</html>\n",
            "index.html": b"<html>index</html>\n",
        },
    )

    selection = publish(LATENCY, FILER, tmp_path, filer)

    assert selection is not None
    assert selection.report == OLDER
    folder = tmp_path / "latency"
    assert (folder / "latest.json").read_bytes() == report
    assert (folder / "latest.html").read_bytes() == b"<html>older</html>\n"
    assert json.loads((folder / "selection.json").read_text()) == selection.as_object()
    assert sorted(path.name for path in folder.iterdir()) == [
        "latest.html",
        "latest.json",
        "selection.json",
    ]


def test_publishing_fails_before_writing_when_the_latency_page_is_missing(tmp_path: Path) -> None:
    filer = filer_with(LATENCY_FOLDER, {NEWEST: encoded(latency_document())})

    with pytest.raises(FileNotFoundError, match=r"20261111T223345Z\.html"):
        publish(LATENCY, FILER, tmp_path, filer)

    assert not (tmp_path / "latency").exists()


def test_publishing_quality_copies_only_the_json_report(tmp_path: Path) -> None:
    report = encoded(quality_document(passed=False))
    filer = filer_with(QUALITY_FOLDER, {NEWEST: report})

    selection = publish(QUALITY, FILER, tmp_path, filer)

    assert selection is not None
    assert (tmp_path / "quality" / "latest.json").read_bytes() == report
    assert sorted(path.name for path in (tmp_path / "quality").iterdir()) == [
        "latest.json",
        "selection.json",
    ]


def test_publishing_without_a_report_writes_nothing(tmp_path: Path) -> None:
    assert publish(QUALITY, FILER, tmp_path, FakeFiler({})) is None
    assert not (tmp_path / "quality").exists()


def test_main_publishes_every_kind_it_finds_and_fails_naming_the_missing_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    filer = filer_with(
        LATENCY_FOLDER, {NEWEST: encoded(latency_document()), "20261111T223345Z.html": b"<p>"}
    )

    code = main(["--filer-url", FILER, "--out", str(tmp_path)], fetch=filer)

    output = capsys.readouterr()
    assert code == 1
    assert (tmp_path / "latency" / "latest.json").exists()
    assert f"latency: {NEWEST} -> {tmp_path / 'latency'}" in output.out
    assert "quality: no data quality report in" in output.err
    assert "make data-quality" in output.err


def test_main_names_the_problem_of_a_selected_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    filer = filer_with(
        LATENCY_FOLDER,
        {NEWEST: encoded(latency_document(passed=False, timeouts=1)), "20261111T223345Z.html": b""},
    )

    code = main(["--filer-url", FILER, "--out", str(tmp_path), "latency"], fetch=filer)

    assert code == 0
    assert "did not pass: 1 timeout" in capsys.readouterr().out


def test_main_names_the_newer_reports_it_skipped(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    filer = filer_with(QUALITY_FOLDER, {NEWEST: b"[]", OLDER: encoded(quality_document())})

    code = main(["--filer-url", FILER, "--out", str(tmp_path), "quality"], fetch=filer)

    assert code == 0
    assert f"skipped {NEWEST}: is not a data quality report" in capsys.readouterr().out


def test_main_fails_naming_a_missing_page_and_copies_nothing_of_that_kind(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    filer = filer_with(LATENCY_FOLDER, {NEWEST: encoded(latency_document())})

    code = main(["--filer-url", FILER, "--out", str(tmp_path), "latency"], fetch=filer)

    assert code == 1
    assert "20261111T223345Z.html is missing; nothing copied" in capsys.readouterr().err
    assert not (tmp_path / "latency").exists()


def test_main_fails_with_exit_code_2_when_the_filer_is_unreachable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def unreachable(url: str) -> bytes | None:
        raise FilerUnavailableError(f"cannot reach {url}")

    code = main(["--filer-url", FILER, "--out", str(tmp_path)], fetch=unreachable)

    assert code == 2
    assert "cannot reach" in capsys.readouterr().err


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        status = {"/ok": 200, "/missing": 404}.get(self.path, 500)
        body = b'{"ok": true}' if status == 200 else b""
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def server() -> Iterator[str]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


def test_http_fetch_returns_the_body(server: str) -> None:
    assert http_fetch(f"{server}/ok") == b'{"ok": true}'


def test_http_fetch_returns_none_for_a_missing_file(server: str) -> None:
    assert http_fetch(f"{server}/missing") is None


def test_http_fetch_fails_on_a_server_error(server: str) -> None:
    with pytest.raises(FilerUnavailableError, match="HTTP 500"):
        http_fetch(f"{server}/broken")


def test_http_fetch_fails_when_nothing_listens() -> None:
    with pytest.raises(FilerUnavailableError, match="cannot reach"):
        http_fetch("http://127.0.0.1:9/ok")


def test_the_folders_and_checks_match_the_jobs_that_write_the_reports() -> None:
    pytest.importorskip("pyspark")
    from pipelines.latency import report as latency_report
    from pipelines.quality import job as quality_job
    from pipelines.quality import report as quality_report

    assert LATENCY.folder == latency_report.REPORTS
    assert QUALITY.folder == quality_report.REPORTS
    assert tuple(quality_job.CHECKS) == QUALITY_CHECKS
