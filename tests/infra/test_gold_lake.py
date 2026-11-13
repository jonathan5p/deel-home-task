import pytest

from tests.infra.gold_lake_setup import COMPARED_VIEWS, LakeComparison


def test_the_gold_job_creates_the_gold_views_and_a_refresh_replaces_none(
    lake_comparison: LakeComparison,
) -> None:
    assert lake_comparison.refreshes == {"views": 14, "replaced": 0}


@pytest.mark.parametrize("view", COMPARED_VIEWS)
def test_each_gold_view_in_the_lake_matches_postgres(
    lake_comparison: LakeComparison, view: str
) -> None:
    compared = lake_comparison.views[view]

    assert compared.rows > 0
    assert (compared.lake_only, compared.postgres_only) == (0, 0)


def test_the_freshness_view_has_a_time_for_every_entity(lake_comparison: LakeComparison) -> None:
    assert lake_comparison.fresh == ["customers", "order_items", "orders", "products"]
