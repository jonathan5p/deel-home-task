import pytest

from tests.prometheus.promql_setup import metric_names, ranges


@pytest.mark.parametrize(
    ("expr", "names"),
    [
        ("up", {"up"}),
        (
            'sum by (app) (rate(a_total{service=~"$service", x="y z"}[$__rate_interval]))',
            {"a_total"},
        ),
        ("a / on(instance) group_left(app) b > bool 1e3", {"a", "b"}),
        ("histogram_quantile(0.95, sum by (le) (rate(c_bucket[5m])))", {"c_bucket"}),
    ],
)
def test_metric_names_are_the_selected_series_only(expr: str, names: set[str]) -> None:
    assert metric_names(expr) == names


def test_ranges_lists_every_range_and_subquery_window() -> None:
    assert ranges("rate(a[$__rate_interval]) + avg_over_time((b > 1)[1h:])") == [
        "$__rate_interval",
        "1h:",
    ]
