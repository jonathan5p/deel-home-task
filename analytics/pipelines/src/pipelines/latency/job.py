import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from common.settings import InvalidSettingError, MissingSettingsError
from pipelines.bronze.entities import ENTITIES, UnknownEntityError
from pipelines.latency.duck import (
    GOLD_ALIAS,
    SOURCE_ALIAS,
    DuckDbSinkLogStore,
    DuckDbTextStore,
    MissingExtensionError,
    connect,
    postgres_runner,
    query_runner,
)
from pipelines.latency.gold import GoldLookup, gold_tables
from pipelines.latency.html import render_index, render_run
from pipelines.latency.lake import lake_lookups
from pipelines.latency.landing import LandingWatch, SinkLog, SinkLogError
from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.measure import HOPS, latencies
from pipelines.latency.probe import (
    LandingSearch,
    LayerLookup,
    Probe,
    ProbeRound,
    ProbeSetupError,
    QuerySource,
    Source,
    Timing,
    probe_targets,
    source_tables,
)
from pipelines.latency.report import (
    ReportPaths,
    RunReport,
    TextStore,
    index_entries,
    index_path,
    report_document,
    report_paths,
)
from pipelines.latency.settings import LatencySettings, ProbeOptions, parse_options
from pipelines.settings import LakeSettings

SUMMARY_PREFIX = "LATENCY"
SETUP_FAILED = 2
SETUP_ERRORS = (
    MissingSettingsError,
    InvalidSettingError,
    ProbeSetupError,
    UnknownEntityError,
    SinkLogError,
    MissingExtensionError,
    duckdb.Error,
)


@dataclass(frozen=True, slots=True)
class ProbeIo:
    """Everything the probe reads or writes: the source database, each topic's landing files,
    the bronze, silver and gold lookups, and the report store."""

    source: Source
    landing: Callable[[str], LandingSearch]
    lookups: Mapping[Layer, LayerLookup]
    store: TextStore


def write_reports(
    store: TextStore, lake: LakeSettings, report: RunReport, now: datetime
) -> ReportPaths:
    """Write the run's JSON and HTML reports, then rebuild the index of every run."""
    paths = report_paths(lake, now)
    store.write_text(paths.json, json.dumps(report_document(report), indent=2) + "\n")
    store.write_text(paths.html, render_run(report))
    store.write_text(index_path(lake), render_index(index_entries(store, lake)))
    return paths


def _cell(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def round_line(probe: ProbeRound) -> str:
    parts = []
    for observation in probe.observations:
        measured = latencies(observation)
        parts.append(
            f"{observation.entity} "
            + " ".join(f"{layer}={_cell(measured[layer])}" for layer in LAYERS)
        )
    return f"{SUMMARY_PREFIX} probe {probe.number}: " + "; ".join(parts)


def console_lines(report: RunReport) -> list[str]:
    """The latency and hop tables, then any timeouts and exceeded thresholds."""
    lines = [
        f"Latency in seconds from the source commit, nearest rank over {len(report.rounds)} probes",
        f"{'entity':<12} {'layer':<8} {'probes':>6} {'min':>7} {'p50':>7} {'p90':>7}"
        f" {'p95':>7} {'max':>7}",
    ]
    for entity, by_layer in report.latency.items():
        for layer, found in by_layer.items():
            values = (
                ["-"] * 6
                if found is None
                else [
                    str(found.count),
                    *(
                        _cell(v)
                        for v in (found.minimum, found.p50, found.p90, found.p95, found.maximum)
                    ),
                ]
            )
            count, *seconds = values
            lines.append(
                f"{entity:<12} {layer:<8} {count:>6} " + " ".join(f"{s:>7}" for s in seconds)
            )
    lines.append("Median seconds per hop")
    lines.append(f"{'entity':<12} " + " ".join(f"{hop:>18}" for hop in HOPS))
    for entity, by_hop in report.hops.items():
        medians = [None if (found := by_hop[hop]) is None else found.p50 for hop in HOPS]
        lines.append(f"{entity:<12} " + " ".join(f"{_cell(m):>18}" for m in medians))
    lines += [
        f"TIMEOUT probe {t.probe} {t.entity}: last layer reached {t.last_layer or 'none'}"
        for t in report.timeouts
    ]
    lines += [
        f"THRESHOLD probe {b.probe} {b.entity} {b.layer}: {b.seconds:.1f} s > {b.limit:g} s"
        for b in report.breaches
    ]
    return lines


def summary_line(report: RunReport, paths: ReportPaths) -> str:
    p95 = {
        entity: {
            str(layer): None if s is None else round(s.p95, 3) for layer, s in by_layer.items()
        }
        for entity, by_layer in report.latency.items()
    }
    summary = {
        "passed": report.passed,
        "probes": len(report.rounds),
        "timeouts": len(report.timeouts),
        "breaches": len(report.breaches),
        "p95_seconds": p95,
        "report": paths.json,
        "html": paths.html,
    }
    return f"{SUMMARY_PREFIX} {json.dumps(summary)}"


def run(
    settings: LatencySettings,
    options: ProbeOptions,
    io: ProbeIo,
    *,
    timing: Timing | None = None,
    out: Callable[[str], None] = print,
) -> int:
    """Probe every selected entity, print and store the report; 0 if every probe reached gold
    within the thresholds, 1 otherwise."""
    timing = timing or Timing()
    started_at = timing.now()
    tables = source_tables(options.entities, settings.topics)
    targets = probe_targets(tables, options.keys, io.source)
    probe = Probe(
        source=io.source,
        landing={table.name: io.landing(table.topic) for table in tables},
        lookups=io.lookups,
        options=options,
        max_wait_seconds=settings.longest_trigger_seconds,
        timing=timing,
    )
    keys = ", ".join(f"{target.name}={target.key}" for target in targets)
    out(f"{SUMMARY_PREFIX} probing {keys}: {options.probes} probes")
    rounds = probe.run(targets, after_round=lambda probe_round: out(round_line(probe_round)))
    report = RunReport(
        started_at=started_at,
        finished_at=timing.now(),
        options=options,
        trigger_seconds=settings.trigger_seconds,
        targets=targets,
        rounds=rounds,
    )
    paths = write_reports(
        io.store, settings.lake, report, datetime.fromtimestamp(started_at / 1000, UTC)
    )
    for line in console_lines(report):
        out(line)
    out(summary_line(report, paths))
    return 0 if report.passed else 1


def duckdb_io(connection: duckdb.DuckDBPyConnection, settings: LatencySettings) -> ProbeIo:
    store = DuckDbTextStore(connection)
    query = query_runner(connection)

    def landing(topic: str) -> LandingWatch:
        log = SinkLog(DuckDbSinkLogStore(store, settings.lake.landing_path(topic)))
        return LandingWatch(log, query)

    return ProbeIo(
        source=QuerySource(postgres_runner(connection, SOURCE_ALIAS)),
        landing=landing,
        lookups={
            **lake_lookups(settings.lake.catalog, query),
            Layer.GOLD: GoldLookup(gold_tables(), postgres_runner(connection, GOLD_ALIAS)),
        },
        store=store,
    )


def main(argv: Sequence[str], env: Mapping[str, str]) -> int:
    options = parse_options(argv, [entity.name for entity in ENTITIES])
    try:
        settings = LatencySettings.from_env(env)
        connection = connect(settings)
        try:
            return run(settings, options, duckdb_io(connection, settings))
        finally:
            connection.close()
    except SETUP_ERRORS as error:
        print(f"{SUMMARY_PREFIX} setup failed: {error}", file=sys.stderr)
        return SETUP_FAILED


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], os.environ))
