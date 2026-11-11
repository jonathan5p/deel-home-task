import pytest

from tests.infra.stack import (
    PROJECT,
    SHARED_GROUP,
    Stack,
    misplaced_scenario_users,
    project_name,
    worker_project,
    xdist_scope,
)


@pytest.mark.parametrize(
    ("folder", "expected"),
    [
        ("deel_home_task", "prefix-deel_home_task"),
        ("feat-23-landing-check", "prefix-feat-23-landing-check"),
        ("Feat.23 Landing+Check", "prefix-feat-23-landing-check"),
    ],
)
def test_project_names_keep_only_compose_safe_characters(folder: str, expected: str) -> None:
    assert project_name("prefix-", folder) == expected


def test_each_xdist_worker_gets_its_own_project() -> None:
    assert worker_project("tests", {"PYTEST_XDIST_WORKER": "gw1"}) == "tests-gw1"


def test_a_run_without_workers_keeps_the_checkout_project() -> None:
    assert worker_project("tests", {}) == "tests"


def test_an_isolated_stack_uses_the_worker_project_and_skips_the_driver_build() -> None:
    stack = Stack.isolated({"PYTEST_XDIST_WORKER": "gw0", "HOME": "/home/a"})

    assert stack.project == f"{PROJECT}-gw0"
    assert stack.env["DRIVER_BUILD"] == ""
    assert stack.env["HOME"] == "/home/a"


def test_a_grouped_module_runs_in_its_group() -> None:
    assert xdist_scope("tests/infra/test_silver.py::test_a[b::c]") == SHARED_GROUP


def test_any_other_module_runs_whole_in_one_worker() -> None:
    assert xdist_scope("tests/infra/test_kafka.py::test_a") == "tests/infra/test_kafka.py"


def test_a_session_scenario_user_outside_the_shared_group_is_misplaced() -> None:
    fixtures = {
        "tests/infra/test_bronze.py": {"stack", "first"},
        "tests/infra/test_new.py": {"stack", "second"},
        "tests/infra/test_kafka.py": {"stack"},
    }

    assert misplaced_scenario_users(fixtures, ("first", "second")) == ["tests/infra/test_new.py"]
