import pytest

from tests.infra.gold_lake_setup import COMPARED_VIEWS, LakeComparison
from tests.infra.silver_setup import SilverRuns


def test_the_views_job_creates_the_gold_views_once(lake_comparison: LakeComparison) -> None:
    assert lake_comparison.refreshes == {"created": 14, "replaced": 0}


@pytest.mark.parametrize("view", COMPARED_VIEWS)
def test_each_gold_view_in_the_lake_matches_postgres(
    lake_comparison: LakeComparison, view: str
) -> None:
    compared = lake_comparison.views[view]

    assert compared.rows > 0
    assert (compared.lake_only, compared.postgres_only) == (0, 0)


def test_the_gold_views_can_be_queried_with_make_spark_sql(silver_runs: SilverRuns) -> None:
    assert silver_runs.gold_spark_sql == {
        "calendar": [["36525"]],
        "statuses": [["5"]],
        "freshness": [
            ["customers", "true"],
            ["order_items", "true"],
            ["orders", "true"],
            ["products", "true"],
        ],
    }
