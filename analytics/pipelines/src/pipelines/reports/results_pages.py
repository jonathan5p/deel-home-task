import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipelines.reports.latest import LATEST, SELECTION

LATENCY_PAGE = "operations/latency-results.md"
QUALITY_PAGE = "lakehouse/data-quality-results.md"
RESULTS_LINK = "../results"
REPORT_NAME_FORMAT = "%Y%m%dT%H%M%SZ.json"
NOT_REACHED = "not reached"
REFRESH = (
    "The site shows the report committed under `docs/results/`;"
    " `make docs-results` replaces it with the newest one in the lake."
)

type Document = Mapping[str, object]


@dataclass(frozen=True, slots=True)
class Committed:
    """A report copied into the docs by `make docs-results`, with why it was selected."""

    document: Document
    report: str
    caveat: str | None
    skipped: tuple[tuple[str, str], ...]


def _mapping(value: object) -> Document:
    return value if isinstance(value, dict) else {}


def _sequence(value: object) -> Sequence[object]:
    return value if isinstance(value, list) else []


def read_committed(folder: Path) -> Committed | None:
    """The report under `folder`, or None when none has been committed."""
    report, selection = folder / f"{LATEST}.json", folder / SELECTION
    if not report.is_file() or not selection.is_file():
        return None
    chosen = _mapping(json.loads(selection.read_text()))
    caveat = chosen.get("caveat")
    return Committed(
        document=_mapping(json.loads(report.read_text())),
        report=str(chosen.get("report")),
        caveat=caveat if isinstance(caveat, str) else None,
        skipped=tuple(
            (str(_mapping(s).get("report")), str(_mapping(s).get("reason")))
            for s in _sequence(chosen.get("skipped"))
        ),
    )


def utc_text(moment: datetime) -> str:
    return f"{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S} UTC"


def iso_utc_text(value: object) -> str:
    return utc_text(datetime.fromisoformat(str(value)))


def latency_calculated_at(document: Document) -> str:
    """The start of the probe run, as its report records it."""
    return iso_utc_text(_mapping(document.get("run")).get("started_at"))


def quality_calculated_at(report: str) -> str:
    """When the run wrote its report, which is named after that UTC time."""
    return utc_text(datetime.strptime(report, REPORT_NAME_FORMAT).replace(tzinfo=UTC))


def cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(header: Sequence[str], rows: Iterable[Sequence[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(cell(h) for h in header) + " |",
        "|" + " --- |" * len(header),
    ]
    lines += ["| " + " | ".join(cell(value) for value in row) + " |" for row in rows]
    return lines


def seconds(value: object) -> str:
    return f"{value:.2f}" if isinstance(value, int | float) else NOT_REACHED


def result_text(passed: object) -> str:
    return "Passed" if passed is True else "**Failed**"


def stamp(calculated_at: str, committed: Committed, what: str) -> list[str]:
    lines = [
        f'!!! info "Calculated on {calculated_at} from report `{committed.report}`"',
        "",
        f"    {what} {REFRESH}",
        "",
    ]
    if committed.caveat:
        lines += [
            '!!! failure "No newer report was complete and passing"',
            "",
            f"    This report {committed.caveat}; it is shown as it is.",
            "",
        ]
    if committed.skipped:
        lines += ['!!! warning "Newer reports not shown"', ""]
        lines += [f"    - `{report}` {reason}" for report, reason in committed.skipped]
        lines.append("")
    return lines


def missing_page(title: str, make_target: str) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            '!!! warning "No report committed yet"',
            "",
            f"    Run `make {make_target}` on the running stack, then `make docs-results` to copy"
            " its report into `docs/results/`.",
            "",
        ]
    )


def _stats(by_layer: object, statistic: str) -> dict[str, object]:
    return {
        str(layer): _mapping(stats).get(statistic) for layer, stats in _mapping(by_layer).items()
    }


def _columns(by_entity: Document) -> list[str]:
    columns: list[str] = []
    for values in by_entity.values():
        columns += [str(name) for name in _mapping(values) if str(name) not in columns]
    return columns


def latency_tables(summary: Document) -> list[str]:
    latency = _mapping(summary.get("latency_seconds"))
    layers = _columns(latency)
    lines = [
        "## Latency from the source commit",
        "",
        "Seconds from the commit in the source database until the layer held the change,"
        " as p50 / p95 over the probes that reached the layer.",
        "",
    ]
    lines += markdown_table(
        ["Entity", *layers],
        (
            [
                entity,
                *(
                    f"{seconds(p50)} / {seconds(_stats(by_layer, 'p95').get(layer))}"
                    if p50 is not None
                    else NOT_REACHED
                    for layer, p50 in _stats(by_layer, "p50").items()
                ),
            ]
            for entity, by_layer in latency.items()
        ),
    )
    hops = _mapping(summary.get("hop_seconds"))
    lines += [
        "",
        "## Hop medians",
        "",
        "Median seconds of each hop of the change on its way to gold.",
        "",
    ]
    lines += markdown_table(
        ["Entity", *(f"`{hop}`" for hop in _columns(hops))],
        (
            [entity, *(seconds(p50) for p50 in _stats(by_hop, "p50").values())]
            for entity, by_hop in hops.items()
        ),
    )
    return lines


def _reached(observation: object) -> str:
    values = _mapping(observation)
    layer = values.get("last_layer")
    if layer is None:
        return NOT_REACHED
    return f"{seconds(_mapping(values.get('latency_seconds')).get(str(layer)))} s at {layer}"


def probe_table(document: Document, entities: Sequence[str]) -> list[str]:
    lines = [
        "## Probes",
        "",
        "Each probe's commit time and, per entity, the seconds until the last layer that held"
        " the change.",
        "",
    ]
    rows = []
    for probe in _sequence(document.get("probes")):
        values = _mapping(probe)
        observations = _mapping(values.get("entities"))
        rows.append(
            [
                values.get("probe"),
                values.get("committed_at"),
                *(_reached(observations.get(entity)) for entity in entities),
            ]
        )
    return lines + markdown_table(["Probe", "Committed at", *entities], rows)


def _pairs(values: object, unit: str = "") -> str:
    pairs = [f"{key}={value}{unit}" for key, value in _mapping(values).items()]
    return ", ".join(pairs) or "none"


def latency_page(committed: Committed | None) -> str:
    title = "Latest latency results"
    if committed is None:
        return missing_page(title, "latency-probe")
    document = committed.document
    run, summary = _mapping(document.get("run")), _mapping(document.get("summary"))
    entities = [str(entity) for entity in _sequence(run.get("entities"))]
    timeouts = [_mapping(t) for t in _sequence(summary.get("timeouts"))]
    breaches = [_mapping(b) for b in _sequence(summary.get("breaches"))]
    what = (
        "The run of `make latency-probe` committed to the repository;"
        f" it finished on {iso_utc_text(run.get('finished_at'))}."
    )
    lines = [f"# {title}", "", *stamp(latency_calculated_at(document), committed, what)]
    lines += [
        f"The [full HTML report]({RESULTS_LINK}/latency/{LATEST}.html) has the chart and every"
        f" probe's timings; the [JSON report]({RESULTS_LINK}/latency/{LATEST}.json) has the raw"
        " numbers. [End-to-end latency](latency.md) explains how the probe measures and how to"
        " read the numbers.",
        "",
        "## Summary",
        "",
    ]
    lines += markdown_table(
        ["Setting or result", "Value"],
        [
            ["Result", result_text(summary.get("passed"))],
            ["Probes", summary.get("probes")],
            ["Entities", ", ".join(entities)],
            ["Timeouts", len(timeouts)],
            ["Limits exceeded", len(breaches)],
            ["Trigger intervals", _pairs(run.get("trigger_seconds"), " s")],
            ["Limits", _pairs(run.get("max_latency_seconds"), " s")],
            ["Poll interval", f"{run.get('poll_seconds')} s"],
            ["Probe timeout", f"{run.get('timeout_seconds')} s"],
            ["Probed keys", _pairs(run.get("keys"))],
        ],
    )
    lines += ["", *latency_tables(summary), "", *probe_table(document, entities)]
    if timeouts:
        lines += ["", "## Timeouts", ""]
        lines += markdown_table(
            ["Probe", "Entity", "Last layer reached"],
            ([t.get("probe"), t.get("entity"), t.get("last_layer") or "none"] for t in timeouts),
        )
    if breaches:
        lines += ["", "## Limits exceeded", ""]
        lines += markdown_table(
            ["Probe", "Entity", "Layer", "Seconds", "Limit"],
            (
                [b.get("probe"), b.get("entity"), b.get("layer"), b.get("seconds"), b.get("limit")]
                for b in breaches
            ),
        )
    return "\n".join(lines) + "\n"


def _summary_text(summary: Document) -> str:
    return (
        f"{result_text(summary.get('passed'))}: {summary.get('checks')} results,"
        f" {summary.get('failed')} failed."
    )


def check_section(name: str, check: Document) -> list[str]:
    results = [_mapping(result) for result in _sequence(check.get("results"))]
    lines = [f"## {name}", "", _summary_text(_mapping(check.get("summary"))), ""]
    lines += markdown_table(
        ["Check", "Subject", "Result", "Metrics"],
        (
            [
                f"`{result.get('check')}`",
                result.get("subject"),
                result_text(result.get("passed")),
                _pairs(result.get("metrics")),
            ]
            for result in results
        ),
    )
    descriptions = {str(r.get("check")): str(r.get("description")) for r in results}
    if descriptions:
        lines += ["", *markdown_table(["Check", "Checks that"], descriptions.items())]
    for result in results:
        if result.get("passed") is not True and result.get("details"):
            details = json.dumps(result.get("details"), indent=2, sort_keys=True)
            lines += [
                "",
                f'??? failure "Details of `{result.get("check")}` on {result.get("subject")}"',
                "",
                "    ```json",
                *(f"    {line}" for line in details.splitlines()),
                "    ```",
            ]
    return lines


def _check_row(name: str, summary: Document) -> list[object]:
    return [
        f"[`{name}`](#{name})",
        result_text(summary.get("passed")),
        summary.get("checks"),
        summary.get("failed"),
    ]


def quality_page(committed: Committed | None) -> str:
    title = "Latest data quality results"
    if committed is None:
        return missing_page(title, "data-quality")
    document = committed.document
    checks = _mapping(document.get("checks"))
    what = (
        f"The run of `make data-quality` committed to the repository, of the checks"
        f" {', '.join(f'`{name}`' for name in checks)}."
    )
    lines = [f"# {title}", "", *stamp(quality_calculated_at(committed.report), committed, what)]
    lines += [
        f"The [JSON report]({RESULTS_LINK}/quality/{LATEST}.json) has every result with its"
        " details. [Data quality checks](data-quality.md) explains what each check compares.",
        "",
        "## Summary",
        "",
        _summary_text(_mapping(document.get("summary"))),
        "",
    ]
    lines += markdown_table(
        ["Check", "Result", "Results", "Failed"],
        (
            _check_row(str(name), _mapping(_mapping(check).get("summary")))
            for name, check in checks.items()
        ),
    )
    for name, check in checks.items():
        lines += ["", *check_section(str(name), _mapping(check))]
    return "\n".join(lines) + "\n"


def results_pages(results: Path) -> dict[str, str]:
    """The pages of the latest results, by path in the docs, from the reports under
    `results/`."""
    return {
        LATENCY_PAGE: latency_page(read_committed(results / "latency")),
        QUALITY_PAGE: quality_page(read_committed(results / "quality")),
    }
