import pytest

from tests.grafana.dashboard_setup import (
    FRESHNESS_PANEL,
    GOLD,
    GOLD_DATASOURCE,
    PANEL_QUERIES,
    panel,
    panels,
    question_query,
    relations,
    targets,
)


def test_there_is_one_panel_per_business_question_and_one_for_freshness() -> None:
    assert sorted(p["title"] for p in panels(GOLD)) == sorted([*PANEL_QUERIES, FRESHNESS_PANEL])


def test_every_query_uses_the_provisioned_gold_datasource() -> None:
    for item, target in targets(GOLD):
        assert item["datasource"]["uid"] == GOLD_DATASOURCE, item["title"]
        assert target["datasource"]["uid"] == GOLD_DATASOURCE, item["title"]
        assert target["format"] == "table", item["title"]


def test_every_query_reads_only_the_public_gold_views() -> None:
    for item, target in targets(GOLD):
        read = relations(target["rawSql"])

        assert read, item["title"]
        assert {name.split(".")[0] for name in read} == {"gold"}, item["title"]


@pytest.mark.parametrize(("title", "file"), PANEL_QUERIES.items())
def test_a_question_panel_runs_the_documented_query(title: str, file: str) -> None:
    [target] = panel(GOLD, title)["targets"]

    assert target["rawSql"].strip() == question_query(file)


def test_the_question_panels_are_tables() -> None:
    assert {panel(GOLD, title)["type"] for title in PANEL_QUERIES} == {"table"}


def test_freshness_shows_one_lag_in_seconds_per_entity_from_the_freshness_view() -> None:
    freshness = panel(GOLD, FRESHNESS_PANEL)
    [target] = freshness["targets"]

    assert freshness["type"] == "stat"
    assert relations(target["rawSql"]) == {"gold.data_freshness"}
    assert freshness["fieldConfig"]["defaults"]["unit"] == "s"
    assert freshness["options"]["reduceOptions"]["values"] is True
    # Each value is named after its entity, so a field filter by name would match nothing.
    assert freshness["options"]["reduceOptions"]["fields"] == ""
