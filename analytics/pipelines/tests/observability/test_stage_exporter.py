import pytest
from prometheus_client import CollectorRegistry

from analytics.pipelines.tests.observability.stage_exporter_setup import (
    APP_NAME,
    APP_PATH,
    QUERY,
    SHUFFLE_READ_STAGE,
    SHUFFLE_WRITE_STAGE,
    FakeHistoryServer,
    applications,
    before_first_stage,
    between_the_stages,
    labels,
    running,
    sample,
    stage,
    summary,
    summary_path,
    with_quantiles,
)
from pipelines.observability.stage_exporter import (
    AppAttempt,
    StageExporter,
    StageMetrics,
    active_attempts,
    data_skew,
    epoch_seconds,
    http_fetch,
    query_name,
    stage_seconds,
    task_duration_skew,
)

STAGES_PATH = f"{APP_PATH}/stages?status=complete"


def exporter(server: FakeHistoryServer, started: float) -> tuple[StageExporter, CollectorRegistry]:
    registry = CollectorRegistry()
    return StageExporter(server, StageMetrics(registry), started), registry


def test_history_server_times_are_utc() -> None:
    assert epoch_seconds("1970-01-01T00:01:00.500GMT") == 60.5


def test_an_application_that_ended_after_the_start_is_active() -> None:
    [attempt] = active_attempts(applications(), since=before_first_stage())

    assert attempt == AppAttempt("app-20261111200737-0000", None, APP_NAME, completed=True)
    assert attempt.path == APP_PATH


def test_an_application_that_ended_before_the_start_is_not_active() -> None:
    ended = applications()[0]["attempts"][0]["endTimeEpoch"] / 1000

    assert active_attempts(applications(), since=ended + 1) == []


def test_a_running_application_is_active_whenever_it_started() -> None:
    [attempt] = active_attempts(running(applications()), since=4_000_000_000)

    assert attempt.completed is False


def test_an_attempt_with_an_id_has_it_in_its_path() -> None:
    assert AppAttempt("app-1", "2", "a", completed=True).path == "applications/app-1/2"


def test_a_stage_belongs_to_the_query_named_on_the_first_line_of_its_description() -> None:
    assert query_name(stage(SHUFFLE_READ_STAGE)) == QUERY


def test_a_batch_stage_belongs_to_no_query() -> None:
    batch = stage(SHUFFLE_READ_STAGE) | {"description": None}

    assert query_name(batch) == ""
    assert query_name({"stageId": 1}) == ""


def test_stage_duration_runs_from_submission_to_completion() -> None:
    assert stage_seconds(stage(SHUFFLE_READ_STAGE)) == pytest.approx(0.514)


def test_task_duration_skew_is_the_slowest_task_over_the_median_one() -> None:
    assert task_duration_skew(summary(SHUFFLE_WRITE_STAGE)) == pytest.approx(124 / 109)


def test_data_skew_reads_the_shuffle_bytes_of_a_stage_that_reads_a_shuffle() -> None:
    skewed = with_quantiles(
        summary(SHUFFLE_READ_STAGE), "shuffleReadMetrics", "readBytes", [10, 100, 300, 400]
    )

    assert data_skew(stage(SHUFFLE_READ_STAGE), skewed) == 4


def test_data_skew_reads_the_input_bytes_of_a_stage_without_shuffle_reads() -> None:
    skewed = with_quantiles(summary(SHUFFLE_WRITE_STAGE), "inputMetrics", "bytesRead", [1, 2, 3, 6])

    assert data_skew(stage(SHUFFLE_WRITE_STAGE), skewed) == 3


def test_there_is_no_data_skew_when_the_median_task_reads_nothing() -> None:
    assert data_skew(stage(SHUFFLE_READ_STAGE), summary(SHUFFLE_READ_STAGE)) is None


def test_a_poll_records_every_stage_completed_after_the_start() -> None:
    stage_exporter, registry = exporter(FakeHistoryServer.captured(), before_first_stage())

    assert stage_exporter.poll() == 2
    assert sample(registry.get_sample_value("spark_stage_duration_seconds_count", labels())) == 2
    assert sample(registry.get_sample_value("spark_stage_tasks_sum", labels())) == 18


def test_a_stage_is_recorded_once() -> None:
    server = FakeHistoryServer.captured(running(applications()))
    stage_exporter, _ = exporter(server, before_first_stage())

    stage_exporter.poll()

    assert stage_exporter.poll() == 0
    assert server.calls.count(summary_path(SHUFFLE_READ_STAGE)) == 1


def test_stages_completed_before_the_start_are_skipped_without_their_summary() -> None:
    server = FakeHistoryServer.captured()
    stage_exporter, _ = exporter(server, between_the_stages())

    assert stage_exporter.poll() == 1
    assert summary_path(SHUFFLE_WRITE_STAGE) not in server.calls


def test_a_completed_application_is_read_once_and_a_running_one_at_every_poll() -> None:
    completed = FakeHistoryServer.captured()
    still_running = FakeHistoryServer.captured(running(applications()))
    for server in (completed, still_running):
        stage_exporter, _ = exporter(server, before_first_stage())
        stage_exporter.poll()
        stage_exporter.poll()

    assert completed.calls.count(STAGES_PATH) == 1
    assert still_running.calls.count(STAGES_PATH) == 2


def test_a_stage_whose_summary_failed_is_recorded_at_the_next_poll() -> None:
    server = FakeHistoryServer.captured()
    server.failing_once.add(summary_path(SHUFFLE_WRITE_STAGE))
    stage_exporter, registry = exporter(server, before_first_stage())

    with pytest.raises(OSError, match="unavailable"):
        stage_exporter.poll()

    assert stage_exporter.poll() == 1
    assert sample(registry.get_sample_value("spark_stage_duration_seconds_count", labels())) == 2


@pytest.mark.parametrize(
    ("metric", "expected"),
    [
        ("spark_stage_run_seconds_total", 3.367),
        ("spark_stage_cpu_seconds_total", 0.159281598),
        ("spark_stage_shuffle_read_local_bytes_total", 395),
        ("spark_stage_shuffle_read_remote_bytes_total", 395),
        ("spark_stage_shuffle_fetch_wait_seconds_total", 0.003),
        ("spark_stage_median_task_seconds_sum", 0.456),
    ],
)
def test_a_stage_adds_its_measures_in_their_units(metric: str, expected: float) -> None:
    registry = CollectorRegistry()

    StageMetrics(registry).record(APP_NAME, stage(SHUFFLE_READ_STAGE), summary(SHUFFLE_READ_STAGE))

    assert sample(registry.get_sample_value(metric, labels())) == pytest.approx(expected)


def test_a_skew_without_a_median_is_not_observed() -> None:
    registry = CollectorRegistry()

    StageMetrics(registry).record(APP_NAME, stage(SHUFFLE_READ_STAGE), summary(SHUFFLE_READ_STAGE))

    assert registry.get_sample_value("spark_stage_data_skew_count", labels()) is None
    assert registry.get_sample_value("spark_stage_task_duration_skew_count", labels()) == 1


def test_http_fetch_reads_json_under_the_rest_root(history_server: str) -> None:
    assert http_fetch(history_server)("applications?status=running") == {
        "path": "/api/v1/applications?status=running"
    }
