import pytest

from tests.prometheus.scrape_setup import (
    JOB_PAGES,
    parse_exposition,
    prometheus_config,
    relabel,
    scraped,
    stored_metric_names,
)

STREAMING_MEASURES = (
    "inputRate_total",
    "processingRate_total",
    "latency",
    "states_rowsTotal",
    "states_usedBytes",
    "eventTime_watermark",
)


def test_every_scrape_job_has_a_page() -> None:
    jobs = {job["job_name"] for job in prometheus_config()["scrape_configs"]}

    assert jobs == set(JOB_PAGES)


def test_prometheus_scrapes_every_ten_seconds() -> None:
    assert prometheus_config()["global"]["scrape_interval"] == "10s"


@pytest.mark.parametrize("measure", STREAMING_MEASURES)
def test_a_streaming_measure_gets_a_query_label_instead_of_the_query_in_its_name(
    measure: str,
) -> None:
    [series] = [
        s for s in scraped("spark-drivers") if s["__name__"] == f"spark_streaming_{measure}"
    ]

    assert series == {"__name__": f"spark_streaming_{measure}", "query": "rate_window_counts"}


def test_no_driver_metric_keeps_the_query_in_its_name() -> None:
    names = {series["__name__"] for series in scraped("spark-drivers")}

    assert not {name for name in names if "streaming" in name} - {
        f"spark_streaming_{measure}" for measure in STREAMING_MEASURES
    }


@pytest.mark.parametrize("job", sorted(set(JOB_PAGES) - {"spark-executors", "spark-stages"}))
def test_the_servlet_duplicates_histograms_timers_and_type_label_are_dropped(job: str) -> None:
    kept = scraped(job)

    assert kept
    assert not [s for s in kept if s["__name__"].endswith("_Number") or "type" in s]
    assert not [s for s in kept if "CodeGenerator" in s["__name__"]]
    assert not [s for s in kept if "listenerProcessingTime" in s["__name__"]]


def test_the_driver_keeps_its_stage_and_task_counters_and_its_heap() -> None:
    names = {series["__name__"] for series in scraped("spark-drivers")}

    assert {
        "metrics_spark_driver_appStatus_stages_completedStages_Count",
        "metrics_spark_driver_appStatus_tasks_failedTasks_Count",
        "metrics_spark_driver_jvm_heap_used_Value",
        "metrics_spark_driver_ExecutorMetrics_TotalGCTime_Value",
    } <= names


def test_executor_metrics_name_the_application_in_an_app_label() -> None:
    [series] = [
        s
        for s in scraped("spark-executors")
        if s["__name__"] == "metrics_executor_activeTasks" and s["executor_id"] == "1"
    ]

    assert series == {
        "__name__": "metrics_executor_activeTasks",
        "app": "SparkUICheck",
        "executor_id": "1",
    }


@pytest.mark.parametrize("measure", ["cores", "runtime_ms"])
def test_application_gauges_get_an_app_label_instead_of_the_run_in_their_name(
    measure: str,
) -> None:
    [series] = [
        s for s in scraped("spark-applications") if s["__name__"] == f"spark_application_{measure}"
    ]

    assert series == {"__name__": f"spark_application_{measure}", "app": "SparkUICheck"}


def test_stored_names_include_the_cluster_gauges_and_up() -> None:
    assert {
        "up",
        "metrics_master_waitingApps_Value",
        "metrics_worker_coresUsed_Value",
        "metrics_worker_memFree_MB_Value",
        "metrics_executor_ProcessTreeJVMRSSMemory_bytes",
    } <= stored_metric_names()


def test_the_exposition_parser_reads_names_and_labels() -> None:
    page = '# HELP x\nmetric_a{k="v", q="a \\"b\\""} 1.0\nmetric_b 2\n'

    assert parse_exposition(page) == [
        {"__name__": "metric_a", "k": "v", "q": 'a \\"b\\"'},
        {"__name__": "metric_b"},
    ]


def test_relabelling_with_an_empty_replacement_removes_the_target_label() -> None:
    rule = {"source_labels": ["missing"], "target_label": "app"}

    assert relabel({"__name__": "m", "app": "a"}, [rule]) == {"__name__": "m"}


def test_relabelling_keeps_only_matching_series_with_keep() -> None:
    rule = {"source_labels": ["__name__"], "regex": "a.*", "action": "keep"}

    assert relabel({"__name__": "b"}, [rule]) is None
    assert relabel({"__name__": "ab"}, [rule]) == {"__name__": "ab"}


def test_the_exporter_series_carry_only_their_own_labels() -> None:
    labels = {frozenset(series) - {"__name__", "le"} for series in scraped("spark-stages")}

    assert labels == {
        frozenset({"app", "query"}),
        frozenset({"namespace", "table"}),
        frozenset({"namespace", "table", "partition"}),
    }
