from analytics.pipelines.tests.observability.schedule_setup import FakeClock, failing_once
from pipelines.observability.schedule import Task, run_tasks


def test_each_task_runs_at_its_own_interval() -> None:
    clock = FakeClock()
    runs: list[str] = []
    tasks = [
        Task("fast", 10, lambda: runs.append(f"fast@{clock.now:.0f}")),
        Task("slow", 30, lambda: runs.append(f"slow@{clock.now:.0f}")),
    ]

    run_tasks(tasks, clock=clock, sleep=clock.sleep, rounds=4)

    assert runs == ["fast@0", "slow@0", "fast@10", "fast@20", "fast@30", "slow@30"]
    assert clock.slept == [10, 10, 10, 10]


def test_a_failing_task_runs_again_at_its_next_turn_and_others_keep_running() -> None:
    clock = FakeClock()
    calls: list[str] = []
    tasks = [
        Task("flaky", 5, failing_once(lambda: calls.append("flaky"))),
        Task("steady", 5, lambda: calls.append("steady")),
    ]

    run_tasks(tasks, clock=clock, sleep=clock.sleep, rounds=2)

    assert calls == ["steady", "flaky", "steady"]
