import pytest

from analytics.pipelines.tests.gold.scd2_setup import (
    DIM_THINGS,
    RENAMED,
    SILVER_COLUMNS,
    THING_ENTITY,
    referencing,
    without,
)
from pipelines.gold.scd2 import Scd2DefinitionError, Scd2Dimension

THINGS = Scd2Dimension(DIM_THINGS, THING_ENTITY, surrogate_key="thing_key", renamed=RENAMED)


def test_the_staged_columns_are_all_but_the_surrogate_key() -> None:
    assert [column.name for column in THINGS.staged_columns] == [
        "thing_id",
        "thing_name",
        "weight",
        "_commit_sequence",
        "_content_hash",
        "_is_deleted",
        "_valid_from",
    ]


def test_each_staged_column_is_loaded_from_its_from_its_rename_or_its_name() -> None:
    assert THINGS.silver_columns == {
        "thing_id": "thing_id",
        "thing_name": "thing_name",
        "weight": "weight_kg",
        "_commit_sequence": "_commit_sequence",
        "_content_hash": "_content_hash",
        "_is_deleted": "_is_deleted",
        "_valid_from": "_changed_at",
    }


def test_the_key_is_the_column_loaded_from_the_entity_key() -> None:
    assert THINGS.key == "thing_id"


def test_the_staging_table_holds_the_staged_columns_with_their_descriptions() -> None:
    staging = THINGS.staging_table

    assert THINGS.relation == "gold_internal.dim_things"
    assert THINGS.staging_relation == "gold_internal.stage_dim_things"
    assert staging.columns == THINGS.staged_columns
    assert "gold_internal.dim_things" in staging.description


@pytest.mark.parametrize("name", ["thing_key", "_is_deleted", "_content_hash", "_valid_from"])
def test_a_table_without_an_scd2_column_is_refused(name: str) -> None:
    with pytest.raises(
        Scd2DefinitionError, match=f"dim_things: the table has no columns named {name}"
    ):
        Scd2Dimension(without(DIM_THINGS, name), THING_ENTITY, "thing_key", RENAMED)


def test_a_business_column_without_a_silver_column_is_refused() -> None:
    with pytest.raises(Scd2DefinitionError, match="no silver column of things for weight"):
        Scd2Dimension(DIM_THINGS, THING_ENTITY, surrogate_key="thing_key")


def test_a_column_from_another_table_is_refused() -> None:
    table = referencing(DIM_THINGS, ("thing_id", "things.thing_id"), ("thing_name", "x.name"))

    with pytest.raises(Scd2DefinitionError, match="no silver column of things for thing_name"):
        Scd2Dimension(table, THING_ENTITY, "thing_key", RENAMED)


def test_a_rename_of_a_column_that_is_not_staged_is_refused() -> None:
    with pytest.raises(Scd2DefinitionError, match="does not stage: thing_key"):
        Scd2Dimension(DIM_THINGS, THING_ENTITY, "thing_key", (*RENAMED, ("thing_key", "x")))


@pytest.mark.parametrize(
    "renamed",
    [(*RENAMED, ("thing_id", "other_id")), (*RENAMED, ("thing_name", "thing_id"))],
    ids=["none", "two"],
)
def test_exactly_one_column_must_come_from_the_entity_key(renamed: tuple[tuple[str, str]]) -> None:
    with pytest.raises(Scd2DefinitionError, match="exactly one column must come from things"):
        Scd2Dimension(DIM_THINGS, THING_ENTITY, "thing_key", renamed)


def test_silver_input_with_every_source_column_is_accepted() -> None:
    THINGS.check_silver(SILVER_COLUMNS)


def test_silver_input_without_a_source_column_is_refused() -> None:
    columns = [name for name in SILVER_COLUMNS if name not in {"weight_kg", "_changed_at"}]

    with pytest.raises(Scd2DefinitionError, match="no columns named _changed_at, weight_kg"):
        THINGS.check_silver(columns)


def test_new_versions_are_appended_from_staging_unless_already_stored() -> None:
    assert THINGS.insert_statement().splitlines() == [
        "INSERT INTO gold_internal.dim_things (",
        "    thing_id,",
        "    thing_name,",
        "    weight,",
        "    _commit_sequence,",
        "    _content_hash,",
        "    _is_deleted,",
        "    _valid_from",
        ")",
        "SELECT",
        "    thing_id,",
        "    thing_name,",
        "    weight,",
        "    _commit_sequence,",
        "    _content_hash,",
        "    _is_deleted,",
        "    _valid_from",
        "FROM gold_internal.stage_dim_things",
        "ON CONFLICT (thing_id, _commit_sequence) DO NOTHING",
    ]


def test_the_merge_only_appends() -> None:
    assert THINGS.merge_statements() == (THINGS.insert_statement(),)


def test_the_versions_view_selects_every_stored_column_then_the_derived_ones() -> None:
    selected = THINGS.versions_query().split("FROM (", 1)[0].splitlines()

    assert THINGS.versions_relation == "gold_internal.dim_things_versions"
    assert selected[:10] == [
        "SELECT",
        "    thing_key,",
        "    thing_id,",
        "    thing_name,",
        "    weight,",
        "    _commit_sequence,",
        "    _content_hash,",
        "    _is_deleted,",
        "    _valid_from,",
        "    _is_folded,",
    ]
    assert "    ) AS _valid_to," in selected
    assert selected[-1].endswith(" AS _is_current")


def test_the_versions_view_reads_every_version_of_each_key_in_commit_order() -> None:
    query = THINGS.versions_query()

    assert "        FROM gold_internal.dim_things\n" in query
    assert "WINDOW by_key AS (PARTITION BY thing_id ORDER BY _commit_sequence)" in query
    assert query.endswith(") AS numbered")


def test_a_version_is_folded_when_its_hash_and_deletion_equal_the_previous_ones() -> None:
    assert (
        "(_content_hash, _is_deleted) IS NOT DISTINCT FROM\n"
        "                (lag(_content_hash) OVER by_key, lag(_is_deleted) OVER by_key)"
        " AS _is_folded"
    ) in THINGS.versions_query()


def test_a_version_ends_where_the_next_unfolded_one_starts_and_only_the_last_is_current() -> None:
    query = THINGS.versions_query()

    assert "count(*) FILTER (WHERE NOT _is_folded) OVER (" in query
    assert (
        "    max(_valid_from) FILTER (WHERE NOT _is_folded) OVER (\n"
        "        PARTITION BY thing_id ORDER BY version_number"
        " RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING\n"
        "    ) AS _valid_to,"
    ) in query
    assert (
        "    NOT _is_folded\n"
        "        AND NOT _is_deleted\n"
        "        AND version_number = max(version_number) OVER (PARTITION BY thing_id)"
        " AS _is_current"
    ) in query
