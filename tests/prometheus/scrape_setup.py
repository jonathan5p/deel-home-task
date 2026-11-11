import re
from collections.abc import Callable, Iterable, Mapping
from functools import cache, partial
from pathlib import Path
from typing import Any

import yaml
from prometheus_client import CollectorRegistry, generate_latest

from analytics.pipelines.tests.observability.stage_exporter_setup import (
    APP_NAME,
    SHUFFLE_READ_STAGE,
    SHUFFLE_WRITE_STAGE,
    stage,
    summary,
    with_quantiles,
)
from analytics.pipelines.tests.observability.table_stats_setup import SMALL, FakeTables, files
from pipelines.observability.stage_exporter import StageMetrics
from pipelines.observability.table_stats import TableMetrics, TableStatsCollector

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "monitoring/prometheus/prometheus.yml"
RULES = REPO_ROOT / "monitoring/prometheus/rules"
SCRAPES = Path(__file__).parent / "scrapes"
SAMPLE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+\S+$")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
GROUP_REFERENCE = re.compile(r"\$(?:\{(\w+)\}|(\w+))")

# Prometheus's configuration is untyped YAML.
type Config = dict[str, Any]
type Series = dict[str, str]


@cache
def prometheus_config() -> Config:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))


def scrape_job(name: str) -> Config:
    [job] = [job for job in prometheus_config()["scrape_configs"] if job["job_name"] == name]
    return job


def rule_files() -> list[Path]:
    return sorted(RULES.glob("*.yml"))


def alert_rules() -> list[Config]:
    return [
        rule
        for path in rule_files()
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]
        for rule in group["rules"]
    ]


def parse_exposition(text: str) -> list[Series]:
    """The series of a page in Prometheus's text format, each as its labels with `__name__`."""
    series = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        sample = SAMPLE.match(line)
        assert sample, line
        labels = dict(LABEL.findall(sample["labels"] or ""))
        series.append({"__name__": sample["name"], **labels})
    return series


def expand(replacement: str, match: re.Match[str]) -> str:
    """Prometheus's `$1` and `${1}` group references; a missing group expands to nothing."""

    def group(reference: re.Match[str]) -> str:
        key = reference[1] or reference[2]
        return (match[int(key)] if key.isdigit() else match[key]) or ""

    return GROUP_REFERENCE.sub(group, replacement)


def relabel(series: Series, rules: Iterable[Mapping[str, Any]]) -> Series | None:
    """Apply relabelling rules as Prometheus does (replace, drop, keep and labeldrop actions);
    None when a rule drops the series."""
    labels = dict(series)
    for rule in rules:
        action = rule.get("action", "replace")
        regex = re.compile(f"(?:{rule.get('regex', '(.*)')})")
        if action == "labeldrop":
            labels = {name: value for name, value in labels.items() if not regex.fullmatch(name)}
            continue
        value = rule.get("separator", ";").join(
            labels.get(name, "") for name in rule.get("source_labels", [])
        )
        match = regex.fullmatch(value)
        if action == "drop" and match:
            return None
        if action == "keep" and not match:
            return None
        if action == "replace" and match:
            replaced = expand(rule.get("replacement", "$1"), match)
            target = rule["target_label"]
            if replaced:
                labels[target] = replaced
            else:
                labels.pop(target, None)
        elif action not in {"replace", "drop", "keep"}:
            raise NotImplementedError(action)
    return labels


def captured(name: str) -> str:
    return (SCRAPES / name).read_text(encoding="utf-8")


def stage_exporter_page() -> str:
    """The exporter's page once it recorded the captured History Server stages, one of them again
    with uneven task reads so that every histogram has observations, and measured a table."""
    registry = CollectorRegistry()
    tables = FakeTables({("bronze", "orders"): files(("day=1", 10), ("day=2", 5_000))})
    stats = TableStatsCollector(tables, TableMetrics(registry), ("bronze",), SMALL)
    stats.collect_tables()
    stats.collect_partitions()
    metrics = StageMetrics(registry)
    for stage_id in (SHUFFLE_WRITE_STAGE, SHUFFLE_READ_STAGE):
        metrics.record(APP_NAME, stage(stage_id), summary(stage_id))
    uneven = with_quantiles(
        summary(SHUFFLE_READ_STAGE), "shuffleReadMetrics", "readBytes", [10, 100, 300, 400]
    )
    metrics.record(APP_NAME, stage(SHUFFLE_READ_STAGE), uneven)
    return generate_latest(registry).decode()


# The page each scrape job reads: those of Spark itself were captured from a Spark 4.1.4 cluster
# running tests/infra/apps/spark_ui_check.py.
JOB_PAGES: Mapping[str, Callable[[], str]] = {
    "spark-drivers": partial(captured, "driver.prom"),
    "spark-executors": partial(captured, "executors.prom"),
    "spark-master": partial(captured, "master.prom"),
    "spark-applications": partial(captured, "applications.prom"),
    "spark-workers": partial(captured, "worker.prom"),
    "spark-stages": stage_exporter_page,
}


def scraped(job: str) -> list[Series]:
    """The series Prometheus stores from the job's page."""
    page = JOB_PAGES[job]()
    rules = scrape_job(job).get("metric_relabel_configs", [])
    return [kept for series in parse_exposition(page) if (kept := relabel(series, rules))]


@cache
def stored_metric_names() -> frozenset[str]:
    """Every metric name Prometheus stores from the scraped pages, plus its own `up`."""
    return frozenset({series["__name__"] for job in JOB_PAGES for series in scraped(job)} | {"up"})
