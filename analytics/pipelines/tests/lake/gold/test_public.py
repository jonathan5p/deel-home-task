import pytest

from pipelines.lake.gold.public import PUBLIC_VIEWS, PublicView, public_columns, view_query
from pipelines.schemas.gold_definitions import load_gold_tables

TABLES = load_gold_tables()
POSTGRES = "gold_internal"
LAKE = "lake.gold_internal"
NAMESPACE = "<namespace>"


@pytest.mark.parametrize("view", PUBLIC_VIEWS, ids=lambda view: view.name)
def test_postgresql_and_the_lake_render_each_view_alike_but_for_the_namespace(
    view: PublicView,
) -> None:
    postgres = view_query(view, TABLES, POSTGRES)
    lake = view_query(view, TABLES, LAKE)

    assert postgres != lake
    assert postgres.replace(f"{POSTGRES}.", f"{NAMESPACE}.") == lake.replace(
        f"{LAKE}.", f"{NAMESPACE}."
    )


@pytest.mark.parametrize("view", PUBLIC_VIEWS, ids=lambda view: view.name)
def test_a_view_reads_only_tables_of_the_namespace_it_is_given(view: PublicView) -> None:
    words = view_query(view, TABLES, NAMESPACE).replace("(", " ").split()

    sources = {
        words[i + 1]
        for i, word in enumerate(words)
        if word in {"FROM", "JOIN"} and words[i + 1] != "SELECT"
    }

    assert sources
    assert all(source.startswith(f"{NAMESPACE}.") for source in sources)


def test_a_projected_view_selects_its_public_columns_with_its_condition() -> None:
    (current_orders,) = (view for view in PUBLIC_VIEWS if view.name == "current_orders")

    lines = view_query(current_orders, TABLES, POSTGRES).splitlines()

    columns = [column.name for column in public_columns(TABLES["current_orders"], ("is_open",))]
    assert lines[0] == "SELECT"
    assert [line.strip(" ,") for line in lines[1:-2]] == columns
    assert lines[-2:] == ["FROM gold_internal.current_orders", "WHERE NOT _is_deleted"]


def test_a_view_with_its_own_query_renders_its_template() -> None:
    query = view_query(PublicView("data_freshness", query=True), TABLES, POSTGRES)

    assert query.startswith("SELECT 'customers' AS entity, max(_valid_from) AS last_changed_at")
    assert "Measured from the data" not in query
