import pytest

from tests.grafana.dashboard_setup import (
    GOLD,
    GOLD_FOLDER,
    LINK_TAG,
    SPARK_AND_LAKE_FOLDER,
    dashboard,
    dashboard_files,
    folder_dashboards,
    provided_files,
    providers,
)

UIDS = [path.stem for path in dashboard_files()]
SPARK_AND_LAKE = folder_dashboards(SPARK_AND_LAKE_FOLDER)


def test_the_gold_dashboard_is_alone_in_the_gold_folder() -> None:
    assert folder_dashboards(GOLD_FOLDER) == [GOLD]


def test_the_spark_and_iceberg_dashboards_share_the_spark_and_lake_folder() -> None:
    assert SPARK_AND_LAKE == ["iceberg-tables", "spark-cluster", "spark-stages", "spark-streaming"]


def test_every_dashboard_is_provisioned_exactly_once() -> None:
    provisioned = [path for provider in providers() for path in provided_files(provider)]

    assert sorted(provisioned) == dashboard_files()


@pytest.mark.parametrize("uid", UIDS)
def test_a_dashboard_has_its_file_name_as_a_stable_uid(uid: str) -> None:
    assert dashboard(uid)["uid"] == uid


@pytest.mark.parametrize("uid", UIDS)
def test_a_dashboard_is_read_only_and_refreshes_every_thirty_seconds(uid: str) -> None:
    model = dashboard(uid)

    assert model["editable"] is False
    assert model["refresh"] == "30s"


@pytest.mark.parametrize("uid", UIDS)
def test_panel_ids_are_unique(uid: str) -> None:
    ids = [p["id"] for p in dashboard(uid)["panels"]]

    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("uid", SPARK_AND_LAKE)
def test_a_spark_and_lake_dashboard_links_to_the_others_through_their_shared_tag(uid: str) -> None:
    model = dashboard(uid)
    [link] = model["links"]

    assert LINK_TAG in model["tags"]
    assert link["type"] == "dashboards"
    assert link["tags"] == [LINK_TAG]
    assert link["asDropdown"] is False
    assert link["keepTime"] is True


def test_the_gold_dashboard_has_no_links_bar() -> None:
    model = dashboard(GOLD)

    assert model["links"] == []
    assert LINK_TAG not in model["tags"]
