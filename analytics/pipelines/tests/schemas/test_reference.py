import re

import pytest

from analytics.pipelines.tests.schemas.reference_setup import (
    BRONZE,
    DIM_THING,
    LAYER,
    LAYER_PAGE,
    THING_FRESHNESS,
    THINGS_PAGE,
    THINGS_RELATION,
)
from analytics.pipelines.tests.schemas.render_setup import SILVER, SILVER_ARCHIVE, THINGS
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.definitions import ColumnType, load_entities
from pipelines.schemas.gold_definitions import load_gold_tables
from pipelines.schemas.reference import (
    LAKE_TYPES,
    LAYERS,
    ReferenceColumn,
    gold_relations,
    index_page,
    lake_relations,
    layer_page,
    reference_pages,
    relation_page,
)

PAGES = reference_pages()
LAKE_SUFFIXES = (("bronze", ("", "_quarantine")), ("silver", ("", "_history", "_current")))


@pytest.mark.parametrize("column_type", list(ColumnType))
def test_lake_types_are_the_spark_sql_names_of_the_types_the_jobs_write(
    column_type: ColumnType,
) -> None:
    assert LAKE_TYPES[column_type] == SPARK_TYPES[column_type].simpleString()


def test_a_lake_layer_lists_each_of_its_templates_for_each_entity_by_name() -> None:
    relations = lake_relations("silver", [SILVER_ARCHIVE, BRONZE, SILVER], [THINGS])

    assert [relation.name for relation in relations] == ["silver.things", "silver.things_archive"]
    assert relations[0].description == "Versions of things: Things"
    assert relations[0].notes == ()


def test_a_lake_column_has_its_lake_name_spark_type_and_nullability() -> None:
    (things,) = lake_relations("silver", [SILVER], [THINGS])

    assert things.columns == (
        ReferenceColumn("thing_id", "bigint", nullable=False, description="Thing identifier"),
        ReferenceColumn("thing_name", "string", nullable=True, description="Name"),
        ReferenceColumn("_price", "decimal(38,10)", nullable=False, description="Price"),
    )


def test_a_partitioned_lake_table_names_its_partition_column() -> None:
    (things,) = lake_relations("bronze", [BRONZE], [THINGS])

    assert things.notes == ("Partitioned by day of `_changed_at`.",)


def test_a_gold_relation_has_its_public_description_and_postgres_types() -> None:
    dim_things, freshness = gold_relations(
        {THING_FRESHNESS.name: THING_FRESHNESS, DIM_THING.name: DIM_THING}
    )

    assert dim_things.name == "gold.dim_things"
    assert dim_things.description == "Things not deleted in the source"
    assert [(column.name, column.type) for column in dim_things.columns] == [
        ("thing_id", "bigint"),
        ("_price", "numeric"),
    ]
    assert dim_things.notes == (
        "Internal table `gold_internal.dim_things`: Things, deleted ones included",
    )
    assert freshness.description == "Newest change of things"
    assert freshness.notes == ()


def test_a_relation_page_has_its_description_notes_and_columns() -> None:
    assert relation_page(THINGS_RELATION) == THINGS_PAGE


def test_a_layer_page_links_each_of_its_relations() -> None:
    assert layer_page(LAYER, [THINGS_RELATION]) == LAYER_PAGE


def test_the_index_links_each_layer_with_its_number_of_relations() -> None:
    text = index_page({LAYER: [THINGS_RELATION]})

    assert text.startswith("# Table reference\n")
    assert text.endswith("| [Bronze](bronze/index.md) | 1 | Typed changes |\n")


def test_the_reference_has_an_index_a_page_per_layer_and_a_page_per_relation() -> None:
    entities = [entity.name for entity in load_entities()]
    lake = {
        f"reference/{layer}/{entity}{suffix}.md"
        for layer, suffixes in LAKE_SUFFIXES
        for entity in entities
        for suffix in suffixes
    }
    gold = {f"reference/gold/{name}.md" for name in load_gold_tables()}
    layers = {f"reference/{layer.name}/index.md" for layer in LAYERS}

    assert set(PAGES) == {"reference/index.md", *layers, *lake, *gold}


def test_every_relation_page_describes_every_column() -> None:
    relation_pages = [text for path, text in PAGES.items() if not path.endswith("index.md")]

    for text in relation_pages:
        rows = [line for line in text.splitlines() if line.startswith("| `")]
        assert rows
        assert all(not re.search(r"\|\s*\|$", row) for row in rows)


def test_every_link_of_a_layer_page_names_a_page_of_that_layer() -> None:
    for layer in LAYERS:
        text = PAGES[f"reference/{layer.name}/index.md"]
        for target in re.findall(r"\]\(([^)/]+\.md)\)", text):
            assert f"reference/{layer.name}/{target}" in PAGES
