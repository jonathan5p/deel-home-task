from pathlib import Path

import pytest

from analytics.pipelines.tests.schemas.reference_setup import (
    BRONZE,
    DIM_THING,
    PAGE,
    PAGE_TABLE,
    THING_FRESHNESS,
    page_files,
)
from analytics.pipelines.tests.schemas.render_setup import SILVER, SILVER_ARCHIVE, THINGS
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.definitions import ColumnType, load_entities
from pipelines.schemas.reference import (
    GENERATED_NOTE,
    LAKE_TYPES,
    ReferenceColumn,
    gold_relations,
    lake_relations,
    main,
    reference_pages,
    render_page,
    rendered_files,
    stale_files,
    write_files,
)

PAGES = reference_pages()


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

    assert dim_things.name == "dim_things"
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


def test_a_page_has_its_title_the_generated_note_and_a_section_per_relation() -> None:
    text = render_page(PAGE)

    assert text.startswith(f"# Bronze tables\n\n{GENERATED_NOTE}\n\n")
    assert text.endswith(PAGE_TABLE)


def test_the_reference_has_one_page_per_layer() -> None:
    assert [page.file_name for page in PAGES] == ["bronze.md", "silver.md", "gold.md"]


def test_the_lake_pages_cover_every_table_and_view_of_every_entity() -> None:
    entities = [entity.name for entity in load_entities()]
    bronze, silver, _ = PAGES

    assert {relation.name for relation in bronze.relations} == {
        f"bronze.{entity}{suffix}" for entity in entities for suffix in ("", "_quarantine")
    }
    assert {relation.name for relation in silver.relations} == {
        f"silver.{entity}{suffix}" for entity in entities for suffix in ("", "_history", "_current")
    }


def test_every_relation_and_column_in_the_reference_is_described() -> None:
    relations = [relation for page in PAGES for relation in page.relations]

    assert all(relation.description.strip() for relation in relations)
    assert all(column.description.strip() for r in relations for column in r.columns)


def test_stale_files_names_missing_changed_and_extra_pages(tmp_path: Path) -> None:
    (tmp_path / "bronze.md").write_text("old", encoding="utf-8")
    (tmp_path / "retired.md").write_text("gone", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a page", encoding="utf-8")

    stale = stale_files(tmp_path, {"bronze.md": "new", "silver.md": "new"})

    assert stale == ["bronze.md", "retired.md", "silver.md"]


def test_a_missing_folder_makes_every_page_stale(tmp_path: Path) -> None:
    assert stale_files(tmp_path / "missing", {"bronze.md": "text"}) == ["bronze.md"]


def test_writing_the_pages_replaces_retired_ones_and_leaves_them_up_to_date(
    tmp_path: Path,
) -> None:
    (tmp_path / "retired.md").write_text("gone", encoding="utf-8")
    files = {"bronze.md": "bronze", "gold.md": "gold"}

    write_files(tmp_path, files)

    assert page_files(tmp_path) == files
    assert stale_files(tmp_path, files) == []


def test_the_command_writes_the_pages_and_its_check_then_passes(tmp_path: Path) -> None:
    out_dir = tmp_path / "tables"

    main([str(out_dir)])
    main([str(out_dir), "--check"])

    assert page_files(out_dir) == rendered_files(PAGES)


def test_the_check_fails_and_writes_nothing_when_a_page_is_out_of_date(tmp_path: Path) -> None:
    main([str(tmp_path)])
    (tmp_path / "gold.md").write_text("edited by hand", encoding="utf-8")

    with pytest.raises(SystemExit, match=r"out of date with the schemas \(gold\.md\)"):
        main([str(tmp_path), "--check"])

    assert (tmp_path / "gold.md").read_text(encoding="utf-8") == "edited by hand"
