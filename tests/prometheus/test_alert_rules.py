import pytest

from tests.prometheus.promql_setup import metric_names
from tests.prometheus.scrape_setup import Config, alert_rules, stored_metric_names

ALERTS = {"StreamingQueryFallingBehind", "StreamingBatchOverTrigger"}


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
