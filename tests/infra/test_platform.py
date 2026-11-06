import json

from tests.infra.stack import MasterApp, Stack, wait_until

SMOKE_APP = "tests/infra/apps/iceberg_smoke_check.py"
VERSIONS_APP = "tests/infra/apps/versions_check.py"
HOLD_APP = "tests/infra/apps/hold_app.py"


def running_with_executors(apps: list[MasterApp]) -> set[str]:
    return {app["name"] for app in apps if app["state"] == "RUNNING" and app["cores"] > 0}


def test_iceberg_smoke_check_writes_catalog_metadata_and_lake_files(stack: Stack) -> None:
    returncode, output = stack.run_app(SMOKE_APP)

    assert returncode == 0, output
    assert "Iceberg smoke check passed" in output
    location = stack.sql(
        "iceberg_catalog",
        "select metadata_location from iceberg_tables"
        " where table_namespace = 'smoke' and table_name = 'iceberg_check'",
    )
    assert location.startswith("s3://lake/warehouse/smoke/iceberg_check/metadata/")
    assert {"data", "metadata"} <= stack.lake_entries("warehouse/smoke/iceberg_check/")


def test_driver_and_executors_run_the_same_spark_and_python(stack: Stack) -> None:
    returncode, output = stack.run_app(VERSIONS_APP)

    assert returncode == 0, output
    line = next(line for line in output.splitlines() if line.startswith("VERSIONS "))
    report = json.loads(line.removeprefix("VERSIONS "))
    assert report["spark"] == "4.1.4"
    assert report["driver_python"].startswith("3.14.")
    assert report["executor_pythons"] == [report["driver_python"]]


def test_each_application_run_gets_its_own_driver_container(stack: Stack) -> None:
    run = stack.start_app(HOLD_APP, "20", conf={"spark.app.name": "hold-single"})

    wait_until(
        lambda: running_with_executors(stack.active_apps()) == {"hold-single"},
        timeout=180,
        message="hold-single never ran with executors",
    )
    assert len(stack.driver_containers()) == 1

    returncode, output = stack.finish(run)
    assert returncode == 0, output
    assert stack.driver_containers() == []


def test_two_applications_run_at_the_same_time(stack: Stack) -> None:
    capped = {"spark.cores.max": "2"}
    first = stack.start_app(HOLD_APP, "40", conf=capped | {"spark.app.name": "hold-a"})
    second = stack.start_app(HOLD_APP, "40", conf=capped | {"spark.app.name": "hold-b"})

    wait_until(
        lambda: running_with_executors(stack.active_apps()) == {"hold-a", "hold-b"},
        timeout=180,
        message="hold-a and hold-b never ran with executors at the same time",
    )
    containers = stack.driver_containers()
    assert len(containers) == 2
    assert len(set(containers)) == 2

    for run in (first, second):
        returncode, output = stack.finish(run)
        assert returncode == 0, output
    assert stack.driver_containers() == []
