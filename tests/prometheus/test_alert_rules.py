import pytest

from tests.prometheus.promql_setup import metric_names
from tests.prometheus.scrape_setup import (
    REPO_ROOT,
    Config,
    alert_rules,
    recording_rules,
    stored_metric_names,
)

ALERTS = {"StreamingQueryFallingBehind", "StreamingBatchOverTrigger"}
PIPELINE_JOBS = ("landing", "bronze", "silver", "gold")


def by_name(name: str) -> Config:
    [rule] = [rule for rule in alert_rules() if rule["alert"] == name]
    return rule


def test_the_streaming_alerts_are_defined() -> None:
    assert {rule["alert"] for rule in alert_rules()} == ALERTS


@pytest.mark.parametrize("name", sorted(ALERTS))
def test_an_alert_fires_only_after_five_minutes(name: str) -> None:
    assert by_name(name)["for"] == "5m"


@pytest.mark.parametrize("name", sorted(ALERTS))
def test_an_alert_reads_only_stored_metrics(name: str) -> None:
    names = metric_names(by_name(name)["expr"])

    assert names
    assert names <= stored_metric_names()


def test_falling_behind_compares_processing_with_input_rate() -> None:
    assert metric_names(by_name("StreamingQueryFallingBehind")["expr"]) == {
        "spark_streaming_processingRate_total",
        "spark_streaming_inputRate_total",
    }


def env_file() -> dict[str, str]:
    lines = (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines()
    return dict(line.split("=", 1) for line in lines if "=" in line and not line.startswith("#"))


def recorded_triggers() -> dict[str, float]:
    return {
        rule["labels"]["service"]: float(rule["expr"].removeprefix("vector(").removesuffix(")"))
        for rule in recording_rules()
        if rule["record"] == "spark_streaming_trigger_seconds"
    }


def test_each_pipeline_job_records_the_trigger_interval_set_in_env() -> None:
    env = env_file()

    assert recorded_triggers() == {
        f"{job}-job": float(env[f"{job.upper()}_TRIGGER_SECONDS"]) for job in PIPELINE_JOBS
    }


def test_the_batch_alert_compares_each_job_with_its_own_trigger() -> None:
    assert metric_names(by_name("StreamingBatchOverTrigger")["expr"]) == {
        "spark_streaming_latency",
        "spark_streaming_trigger_seconds",
    }
