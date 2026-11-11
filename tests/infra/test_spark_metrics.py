from collections.abc import Iterator

import pytest

from tests.infra.grafana_setup import request, start_grafana
from tests.infra.prometheus_setup import (
    query,
    remove_prometheus,
    restart_prometheus,
    run_rule_tests,
    start_prometheus,
)
from tests.infra.stack import Stack, wait_until

STREAMING_APP = "tests/infra/apps/spark_ui_check.py"
APP_NAME = "SparkUICheck"
STREAMING_QUERY = "rate_window_counts"
INPUT_RATE = f'spark_streaming_inputRate_total{{service="spark-app", query="{STREAMING_QUERY}"}}'
EXECUTOR_CORES = f'metrics_executor_totalCores{{app="{APP_NAME}", executor_id!="driver"}}'


def wait_for_series(stack: Stack, promql: str) -> None:
    wait_until(
        lambda: bool(query(stack, promql)),
        timeout=180,
        message=f"Prometheus never stored {promql}",
    )


@pytest.fixture(scope="module")
def metrics(stack: Stack) -> Iterator[Stack]:
    """Prometheus and Grafana, with the one-minute streaming check app scraped once rows flow."""
    start_prometheus(stack)
    start_grafana(stack)
    run = stack.start_app(STREAMING_APP)
    try:
        wait_for_series(stack, f"{INPUT_RATE} > 0")
        wait_for_series(stack, EXECUTOR_CORES)
        yield stack
        returncode, output = stack.finish(run)
        assert returncode == 0, output
    finally:
        remove_prometheus(stack)


def test_a_streaming_query_is_stored_under_its_service_and_query(metrics: Stack) -> None:
    [series] = query(metrics, INPUT_RATE)

    assert series["metric"]["job"] == "spark-drivers"


def test_the_running_driver_is_scraped_under_its_compose_service(metrics: Stack) -> None:
    assert query(metrics, 'up{service="spark-app"} == 1')


def test_executor_metrics_are_stored_per_application(metrics: Stack) -> None:
    assert {series["metric"]["executor_id"] for series in query(metrics, EXECUTOR_CORES)}


def test_the_master_the_applications_page_and_both_workers_are_scraped(metrics: Stack) -> None:
    up = query(metrics, 'up{job=~"spark-master|spark-applications|spark-workers"} == 1')

    assert len(up) == 4


def test_grafanas_prometheus_datasource_connects(metrics: Stack) -> None:
    health = request(metrics, "/api/datasources/uid/prometheus/health")

    assert isinstance(health, dict)
    assert health["status"] == "OK"


def test_the_alert_rules_pass_their_promtool_tests(metrics: Stack) -> None:
    run = run_rule_tests(metrics)

    assert run.returncode == 0, run.stdout + run.stderr


def test_metrics_survive_a_restart_of_prometheus(metrics: Stack) -> None:
    restart_prometheus(metrics)

    assert query(metrics, f"last_over_time({INPUT_RATE}[10m])")
