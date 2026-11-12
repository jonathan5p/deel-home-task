import argparse
import json
import re
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# `make docs-results` runs this on the host, where PySpark is not installed, so nothing is imported
# from the jobs that write the reports; a unit test checks the folders and checks against them.
QUALITY_CHECKS = ("landing", "bronze", "silver", "gold")
REPORT_NAME = re.compile(r"\d{8}T\d{6}Z\.json")
LATEST = "latest"
SELECTION = "selection.json"
LIST_LIMIT = 1000
FETCH_TIMEOUT_SECONDS = 30

type Fetch = Callable[[str], bytes | None]
"""The body at a URL, or None when nothing is there."""


class FilerUnavailableError(Exception):
    pass


def _plural(count: int, noun: str, plural: str | None = None) -> str:
    return f"{count} {noun if count == 1 else plural or noun + 's'}"


def latency_problem(document: object) -> str | None:
    """Why a latency report is not one to show, or None for a run that passed."""
    run = document.get("run") if isinstance(document, dict) else None
    summary = document.get("summary") if isinstance(document, dict) else None
    if not (
        isinstance(run, dict)
        and isinstance(run.get("started_at"), str)
        and isinstance(summary, dict)
        and isinstance(summary.get("passed"), bool)
        and isinstance(summary.get("timeouts"), list)
        and isinstance(summary.get("breaches"), list)
    ):
        return LATENCY.invalid
    if summary["passed"]:
        return None
    timeouts = _plural(len(summary["timeouts"]), "timeout")
    breaches = _plural(len(summary["breaches"]), "limit exceeded", "limits exceeded")
    return f"did not pass: {timeouts}, {breaches}"


def quality_problem(document: object) -> str | None:
    """Why a data quality report is not one to show, or None for a run of every check, whether
    the checks passed or not."""
    checks = document.get("checks") if isinstance(document, dict) else None
    summary = document.get("summary") if isinstance(document, dict) else None
    if not (
        isinstance(checks, dict)
        and isinstance(summary, dict)
        and isinstance(summary.get("passed"), bool)
    ):
        return QUALITY.invalid
    missing = [name for name in QUALITY_CHECKS if name not in checks]
    if missing:
        return f"is not a complete run: it lacks {', '.join(missing)}"
    return None


@dataclass(frozen=True, slots=True)
class ReportKind:
    name: str
    folder: str
    title: str
    make_target: str
    problem: Callable[[object], str | None]
    pages: tuple[str, ...] = ()
    """Suffixes of the files written next to each JSON report that are copied with it."""

    @property
    def invalid(self) -> str:
        return f"is not a {self.title}"


LATENCY = ReportKind(
    "latency", "latency/reports", "latency report", "latency-probe", latency_problem, (".html",)
)
QUALITY = ReportKind(
    "quality", "quality/reports", "data quality report", "data-quality", quality_problem
)
KINDS = {kind.name: kind for kind in (LATENCY, QUALITY)}


@dataclass(frozen=True, slots=True)
class Skipped:
    report: str
    reason: str


@dataclass(frozen=True, slots=True)
class Selection:
    report: str
    content: bytes
    skipped: tuple[Skipped, ...]
    """The reports newer than the selected one, with why each was not selected."""
    caveat: str | None
    """The selected report's own problem, when no report was free of one."""

    def as_object(self) -> dict[str, object]:
        return {
            "report": self.report,
            "caveat": self.caveat,
            "skipped": [{"report": s.report, "reason": s.reason} for s in self.skipped],
        }


def newest_first(names: Iterable[str]) -> list[str]:
    """The JSON reports among `names`, newest first; reports are named after their UTC time."""
    return sorted((name for name in names if REPORT_NAME.fullmatch(name)), reverse=True)


def _parsed(content: bytes) -> object:
    try:
        return json.loads(content)
    except ValueError:
        return None


def select(
    kind: ReportKind, names: Sequence[str], read: Callable[[str], bytes | None]
) -> Selection | None:
    """The newest report without a problem; failing that, the newest report of the kind with its
    problem as a caveat; None if there is no report of the kind."""
    skipped: list[Skipped] = []
    fallback: Selection | None = None
    for name in names:
        content = read(name)
        if content is None:
            continue
        problem = kind.problem(_parsed(content))
        if problem is None:
            return Selection(name, content, tuple(skipped), None)
        if fallback is None and problem != kind.invalid:
            fallback = Selection(name, content, tuple(skipped), problem)
        skipped.append(Skipped(name, problem))
    return fallback


def list_names(fetch: Fetch, folder_url: str) -> list[str]:
    """The names of the files in a SeaweedFS filer folder, following its paged listing."""
    names: list[str] = []
    last = ""
    while True:
        query = f"limit={LIST_LIMIT}" + (f"&lastFileName={quote(last)}" if last else "")
        body = fetch(f"{folder_url}/?{query}")
        listing = _parsed(body) if body is not None else None
        if not isinstance(listing, dict):
            return names
        for entry in listing.get("Entries") or []:
            names.append(str(entry["FullPath"]).rsplit("/", 1)[-1])
        last = str(listing.get("LastFileName") or "")
        if not listing.get("ShouldDisplayLoadMore") or not last:
            return names


def publish(kind: ReportKind, filer_url: str, out: Path, fetch: Fetch) -> Selection | None:
    """Copy the selected report of a kind, and its pages, to `out/<kind>/latest.*`."""
    folder = f"{filer_url}/{kind.folder}"
    selection = select(
        kind, newest_first(list_names(fetch, folder)), lambda n: fetch(f"{folder}/{n}")
    )
    if selection is None:
        return None
    stem = selection.report.removesuffix(".json")
    pages: dict[str, bytes] = {}
    for suffix in kind.pages:
        page = fetch(f"{folder}/{stem}{suffix}")
        if page is None:
            raise FileNotFoundError(f"{folder}/{stem}{suffix} is missing")
        pages[suffix] = page
    target = out / kind.name
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{LATEST}.json").write_bytes(selection.content)
    for suffix, page in pages.items():
        (target / f"{LATEST}{suffix}").write_bytes(page)
    (target / SELECTION).write_text(json.dumps(selection.as_object(), indent=2) + "\n")
    return selection


def http_fetch(url: str) -> bytes | None:
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
            return response.read()
    except HTTPError as error:
        if error.code == 404:
            return None
        raise FilerUnavailableError(f"{url} answered HTTP {error.code}") from error
    except URLError as error:
        raise FilerUnavailableError(f"cannot reach {url}: {error.reason}") from error


def main(argv: Sequence[str], fetch: Fetch = http_fetch) -> int:
    parser = argparse.ArgumentParser(prog="docs-results")
    parser.add_argument("--filer-url", required=True, help="the lake bucket on the filer")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("kinds", nargs="*", choices=list(KINDS), metavar="kind")
    args = parser.parse_args(argv)
    missing = False
    try:
        for name in args.kinds or KINDS:
            kind = KINDS[name]
            try:
                selection = publish(kind, args.filer_url, args.out, fetch)
            except FileNotFoundError as error:
                missing = True
                print(f"{kind.name}: {error}; nothing copied", file=sys.stderr)
                continue
            if selection is None:
                missing = True
                print(
                    f"{kind.name}: no {kind.title} in {args.filer_url}/{kind.folder}/;"
                    f" run make {kind.make_target} first",
                    file=sys.stderr,
                )
                continue
            print(f"{kind.name}: {selection.report} -> {args.out / kind.name}")
            for skipped in selection.skipped:
                print(f"{kind.name}: skipped {skipped.report}: {skipped.reason}")
            if selection.caveat:
                print(f"{kind.name}: {selection.report} {selection.caveat}")
    except FilerUnavailableError as error:
        print(f"{error}; is the lake's filer (seaweedfs) running?", file=sys.stderr)
        return 2
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
