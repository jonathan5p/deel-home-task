from collections.abc import Iterator

import pytest

from tests.infra.prometheus_setup import (
    query,
    remove_prometheus,
    remove_stage_exporter,
    start_prometheus,
    start_stage_exporter,
)
from tests.infra.stack import Stack, wait_until

HOLD_APP = "tests/infra/apps/hold_app.py"
APP_NAME = "stage-exporter-check"
STAGES = f'spark_stage_duration_seconds_count{{app="{APP_NAME}"}}'


@pytest.fixture(scope="module")
def stages(stack: Stack) -> Iterator[Stack]:
    """The History Server, the stage exporter and Prometheus, then a batch app run after them,
    whose stages Prometheus stores once the History Server has read its event log."""
    start_stage_exporter(stack)
    start_prometheus(stack)
    try:
        run = stack.start_app(HOLD_APP, "1", conf={"spark.app.name": APP_NAME})
        returncode, output = stack.finish(run)
        assert returncode == 0, output
        wait_until(
            lambda: bool(query(stack, f"{STAGES} > 0")),
            timeout=180,
            message=f"Prometheus never stored {STAGES}",
        )
        yield stack
    finally:
        remove_prometheus(stack)
        remove_stage_exporter(stack)


def test_a_batch_applications_stages_are_stored_under_its_name_alone(stages: Stack) -> None:
    [series] = query(stages, STAGES)

    assert series["metric"]["job"] == "spark-stages"
    assert "query" not in series["metric"]
    assert float(series["value"][1]) >= 1


@pytest.mark.parametrize(
    "metric",
    [
        "spark_stage_run_seconds_total",
        "spark_stage_cpu_seconds_total",
        "spark_stage_tasks_count",
        "spark_stage_median_task_seconds_count",
        "spark_stage_task_duration_skew_count",
    ],
)
def test_a_stage_measure_is_stored(stages: Stack, metric: str) -> None:
    assert query(stages, f'{metric}{{app="{APP_NAME}"}} > 0')
