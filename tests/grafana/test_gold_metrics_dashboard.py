import pytest

from tests.grafana.dashboard_setup import (
    DATASOURCE_UID,
    FRESHNESS_PANEL,
    PANEL_QUERIES,
    dashboard,
    panel,
    panels,
    question_query,
    relations,
    targets,
)


def test_the_dashboard_has_a_stable_uid_and_refreshes_every_thirty_seconds() -> None:
    model = dashboard()

    assert model["uid"] == "gold-metrics"
    assert model["refresh"] == "30s"
    assert model["editable"] is False


def test_there_is_one_panel_per_business_question_and_one_for_freshness() -> None:
    assert sorted(p["title"] for p in panels()) == sorted([*PANEL_QUERIES, FRESHNESS_PANEL])


def test_panel_ids_are_unique() -> None:
    ids = [p["id"] for p in panels()]

    assert len(ids) == len(set(ids))


def test_every_query_uses_the_provisioned_gold_datasource() -> None:
    for item, target in targets():
        assert item["datasource"]["uid"] == DATASOURCE_UID, item["title"]
        assert target["datasource"]["uid"] == DATASOURCE_UID, item["title"]
        assert target["format"] == "table", item["title"]


def test_every_query_reads_only_the_public_gold_views() -> None:
    for item, target in targets():
        read = relations(target["rawSql"])

        assert read, item["title"]
        assert {name.split(".")[0] for name in read} == {"gold"}, item["title"]


@pytest.mark.parametrize(("title", "file"), PANEL_QUERIES.items())
def test_a_question_panel_runs_the_documented_query(title: str, file: str) -> None:
    [target] = panel(title)["targets"]

    assert target["rawSql"].strip() == question_query(file)


def test_the_question_panels_are_tables() -> None:
    assert {panel(title)["type"] for title in PANEL_QUERIES} == {"table"}


def test_freshness_shows_one_lag_in_seconds_per_entity_from_the_freshness_view() -> None:
    freshness = panel(FRESHNESS_PANEL)
    [target] = freshness["targets"]

    assert freshness["type"] == "stat"
    assert relations(target["rawSql"]) == {"gold.data_freshness"}
    assert freshness["fieldConfig"]["defaults"]["unit"] == "s"
    assert freshness["options"]["reduceOptions"]["values"] is True
