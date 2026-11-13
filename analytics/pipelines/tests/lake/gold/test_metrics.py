import json
import logging

import pytest

from analytics.pipelines.tests.lake.gold.metrics_setup import FakeJobs, Ticks
from pipelines.lake.gold.metrics import BATCH_PREFIX, LOGGER, BatchRun, no_jobs


def test_each_step_records_its_time_and_the_jobs_it_ran() -> None:
    jobs = FakeJobs({1})
    run = BatchRun("fact_order_history", 4, jobs, Ticks())

    with run.step("read"):
        jobs.run(2, 3)
    with run.step("write"):
        jobs.run(4)

    assert run.summary() == {
        "load": "fact_order_history",
        "batch_id": 4,
        "seconds": 5.0,
        "jobs": 3,
        "written": {},
        "steps": [
            {"step": "read", "seconds": 1.0, "jobs": 2},
            {"step": "write", "seconds": 1.0, "jobs": 1},
        ],
    }


def test_a_failing_step_is_still_recorded() -> None:
    run = BatchRun("dim_customer", None, clock=Ticks())

    with pytest.raises(RuntimeError), run.step("write"):
        raise RuntimeError("commit failed")

    assert [step.name for step in run.steps] == ["write"]


def test_jobs_the_tracker_dropped_are_not_counted_again() -> None:
    jobs = FakeJobs({1, 2})
    run = BatchRun("dim_product", 1, jobs, Ticks())

    with run.step("read"):
        jobs.ids = {3}

    assert run.steps[0].jobs == 1


def test_the_batch_is_logged_as_one_json_line(caplog: pytest.LogCaptureFixture) -> None:
    run = BatchRun("dim_customer", 2, clock=Ticks())
    run.written["dim_customer"] = 3

    with caplog.at_level(logging.INFO, logger=LOGGER):
        run.log()

    (record,) = caplog.records
    assert record.getMessage().startswith(BATCH_PREFIX)
    logged = json.loads(record.getMessage().removeprefix(BATCH_PREFIX))
    assert logged["written"] == {"dim_customer": 3}
    assert logged["load"] == "dim_customer"


def test_without_a_tracker_no_jobs_are_counted() -> None:
    assert no_jobs() == frozenset()
