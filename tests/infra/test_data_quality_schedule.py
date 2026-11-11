from tests.infra.data_quality_schedule_setup import first_scheduled_run, run_summary
from tests.infra.stack import Stack


def test_the_scheduled_service_runs_every_check_and_writes_a_report(stack: Stack) -> None:
    summary = run_summary(first_scheduled_run(stack))

    report = str(summary["report"])
    folder, name = report.rsplit("/", 1)
    assert summary["summary"] is True
    assert folder.endswith("/quality/reports")
    assert name in stack.lake_entries("quality/reports")
