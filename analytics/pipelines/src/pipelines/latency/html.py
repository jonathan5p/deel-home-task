from collections.abc import Sequence

from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.measure import HOPS, last_layer, latencies
from pipelines.latency.probe import MARKER_COLUMN
from pipelines.latency.report import IndexEntry, RunReport, iso_time
from pipelines.reports.html import (
    Html,
    badge,
    bar_chart,
    facts,
    join,
    link,
    page,
    section,
    table,
)

HOP_NOTE = (
    "commit_to_debezium: Debezium ts_ms minus the source commit time; debezium_to_kafka: the"
    " Kafka record timestamp minus Debezium ts_ms; each later hop: the first poll that saw the"
    " change in a layer minus the moment it reached the layer before."
)
LATENCY_NOTE = (
    "Seconds from the source commit (Debezium source.ts_ms) to the first poll that saw the"
    " change; nearest-rank percentiles over the probes that reached the layer. Lake gold views"
    " are computed from silver at read time, so they match silver."
)


def _seconds(value: float) -> str:
    return f"{value:g} s"


def _summary(report: RunReport) -> Html:
    options = report.options
    thresholds = ", ".join(f"{layer} ≤ {_seconds(s)}" for layer, s in options.max_latency.items())
    items: list[tuple[str, object]] = [
        ("Status", badge(report.passed)),
        ("Started", iso_time(report.started_at)),
        ("Finished", iso_time(report.finished_at)),
        ("Entities", ", ".join(options.entities)),
        ("Probes", len(report.rounds)),
        ("Poll interval", _seconds(options.poll_seconds)),
        ("Timeout per probe", _seconds(options.timeout_seconds)),
        (
            "Trigger intervals",
            ", ".join(f"{layer} {_seconds(s)}" for layer, s in report.trigger_seconds.items()),
        ),
        ("Thresholds", thresholds or "none"),
        ("Probe keys", ", ".join(f"{name}={key}" for name, key in report.keys.items())),
        ("Marker column", MARKER_COLUMN),
    ]
    parts = [facts(items)]
    if report.timeouts:
        rows = [[t.probe, t.entity, t.last_layer or "none"] for t in report.timeouts]
        parts.append(table(["Probe", "Entity", "Last layer reached"], rows, caption="Timeouts"))
    if report.breaches:
        rows = [[b.probe, b.entity, str(b.layer), b.seconds, b.limit] for b in report.breaches]
        headers = ["Probe", "Entity", "Layer", "Seconds", "Limit"]
        parts.append(table(headers, rows, caption="Thresholds exceeded"))
    return section("Summary", *parts)


def _threshold_status(report: RunReport, entity: str, layer: Layer) -> object:
    if layer not in report.options.max_latency:
        return None
    exceeded = any(b.entity == entity and b.layer is layer for b in report.breaches)
    missed = any(
        t.entity == entity
        and (t.last_layer is None or LAYERS.index(t.last_layer) < LAYERS.index(layer))
        for t in report.timeouts
    )
    return badge(not exceeded and not missed)


def _latency(report: RunReport) -> Html:
    rows: list[list[object]] = []
    bars: list[tuple[str, float]] = []
    for entity, by_layer in report.latency.items():
        for layer, found in by_layer.items():
            values: list[object] = [None] * 6
            if found is not None:
                values = [
                    found.count,
                    found.minimum,
                    found.p50,
                    found.p90,
                    found.p95,
                    found.maximum,
                ]
                bars.append((f"{entity} {layer}", found.p50))
            limit = report.options.max_latency.get(layer)
            rows.append(
                [entity, str(layer), *values, limit, _threshold_status(report, entity, layer)]
            )
    headers = ["Entity", "Layer", "Probes", "Min", "p50", "p90", "p95", "Max", "Limit", "Status"]
    return section(
        "Latency from the source commit (seconds)",
        table(headers, rows, caption=LATENCY_NOTE),
        bar_chart(bars, unit="s", caption="Median (p50) latency by entity and layer"),
    )


def _hops(report: RunReport) -> Html:
    rows: list[list[object]] = []
    for entity, by_hop in report.hops.items():
        for hop in HOPS:
            found = by_hop[hop]
            values = [None] * 3 if found is None else [found.p50, found.p95, found.maximum]
            rows.append([entity, str(hop), *values])
    return section(
        "Hop breakdown (seconds)",
        table(["Entity", "Hop", "p50", "p95", "Max"], rows, caption=HOP_NOTE),
    )


def _probes(report: RunReport) -> Html:
    rows: list[list[object]] = []
    for probe in report.rounds:
        for observation in probe.observations:
            change = observation.change
            measured = latencies(observation)
            reached = last_layer(observation)
            rows.append(
                [
                    probe.number,
                    observation.entity,
                    observation.key,
                    None if change is None else change.commit_sequence,
                    None if change is None else iso_time(change.changed_at),
                    *(measured[layer] for layer in LAYERS),
                    None if reached is None else str(reached),
                ]
            )
    headers = ["Probe", "Entity", "Key", "Commit sequence", "Committed", *LAYERS, "Last layer"]
    return section("Probes (seconds from the source commit)", table(headers, rows))


def render_run(report: RunReport) -> str:
    title = "Latency probe"
    subtitle = f"Run of {iso_time(report.started_at)}, {'passed' if report.passed else 'failed'}"
    parts = (_summary(report), _latency(report), _hops(report), _probes(report))
    return page(title, *parts, subtitle=subtitle)


def render_index(entries: Sequence[IndexEntry]) -> str:
    """The list of runs, newest first, each linking to its HTML and JSON report."""
    rows = [
        [
            link(entry.html, entry.name),
            entry.started_at,
            badge(entry.passed),
            entry.probes,
            ", ".join(entry.entities),
            entry.timeouts,
            entry.breaches,
            link(f"{entry.name}.json", "JSON"),
        ]
        for entry in entries
    ]
    headers = ["Run", "Started", "Status", "Probes", "Entities", "Timeouts", "Breaches", "Data"]
    return page("Latency probe runs", join([table(headers, rows)]), subtitle=f"{len(entries)} runs")
