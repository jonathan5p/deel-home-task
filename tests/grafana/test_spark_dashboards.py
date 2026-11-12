import pytest

from tests.grafana.dashboard_setup import (
    PROMETHEUS_DATASOURCE,
    SPARK_AND_LAKE_FOLDER,
    folder_dashboards,
    panel,
    panels,
    targets,
    variable_selectors,
)
from tests.prometheus.promql_setup import metric_names, ranges
from tests.prometheus.scrape_setup import stored_metric_names

TABLES = "iceberg-tables"
SPARK_AND_LAKE = folder_dashboards(SPARK_AND_LAKE_FOLDER)
SPARK = [uid for uid in SPARK_AND_LAKE if uid != TABLES]
GIB = 1024**3
TRIGGER_SECONDS = 10


def threshold(uid: str, title: str) -> float:
    steps = panel(uid, title)["fieldConfig"]["defaults"]["thresholds"]["steps"]
    return steps[-1]["value"]


@pytest.mark.parametrize("uid", SPARK_AND_LAKE)
def test_every_query_uses_the_provisioned_prometheus_datasource(uid: str) -> None:
    for item, target in targets(uid):
        assert item["datasource"]["uid"] == PROMETHEUS_DATASOURCE, item["title"]
        assert target["datasource"]["uid"] == PROMETHEUS_DATASOURCE, item["title"]


@pytest.mark.parametrize("uid", SPARK_AND_LAKE)
def test_every_query_reads_only_metrics_prometheus_stores(uid: str) -> None:
    for item, target in targets(uid):
        names = metric_names(target["expr"])

        assert names, item["title"]
        assert names <= stored_metric_names(), item["title"]


@pytest.mark.parametrize("uid", SPARK)
def test_every_range_uses_the_rate_interval(uid: str) -> None:
    for item, target in targets(uid):
        assert set(ranges(target["expr"])) <= {"$__rate_interval", "$__rate_interval:"}, item[
            "title"
        ]


@pytest.mark.parametrize("uid", SPARK_AND_LAKE)
def test_every_variable_reads_a_metric_prometheus_stores(uid: str) -> None:
    for name, selector in variable_selectors(uid).items():
        assert metric_names(selector) <= stored_metric_names(), name


@pytest.mark.parametrize("uid", SPARK_AND_LAKE)
def test_every_panel_describes_what_it_shows(uid: str) -> None:
    assert all(item["description"] for item in panels(uid))


def test_the_streaming_dashboard_filters_by_service_and_query() -> None:
    assert set(variable_selectors("spark-streaming")) == {"service", "query"}
    for item, target in targets("spark-streaming"):
        assert 'service=~"$service"' in target["expr"], item["title"]


def test_the_stages_dashboard_filters_by_app_and_query() -> None:
    assert set(variable_selectors("spark-stages")) == {"app", "query"}
    for item, target in targets("spark-stages"):
        assert 'app=~"$app"' in target["expr"], item["title"]


def test_executor_memory_is_shown_against_the_one_gib_heap() -> None:
    assert threshold("spark-cluster", "Executor peak heap and RSS vs 1 GiB") == GIB


def test_batch_duration_is_shown_against_the_trigger() -> None:
    assert threshold("spark-streaming", "Batch duration vs the 10 s trigger") == TRIGGER_SECONDS


def test_the_table_dashboard_shows_hourly_file_changes_and_otherwise_rate_intervals() -> None:
    windows = {window for _, target in targets(TABLES) for window in ranges(target["expr"])}

    assert windows == {"1h"}
    assert (
        "delta(iceberg_table_data_files"
        in panel(TABLES, "Data files added or removed per hour")["targets"][0]["expr"]
    )
