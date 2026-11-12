import pytest

from analytics.pipelines.tests.lake.gold.scd2_setup import (
    DIM_THINGS,
    RENAMED,
    SILVER_COLUMNS,
    THING_ENTITY,
    referencing,
    without,
)
from pipelines.lake.gold.scd2 import Scd2DefinitionError, Scd2Dimension

THINGS = Scd2Dimension(DIM_THINGS, THING_ENTITY, surrogate_key="thing_key", renamed=RENAMED)


def test_the_staged_columns_are_all_but_the_surrogate_key_and_the_version_columns() -> None:
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


def test_the_dimension_is_stored_in_the_internal_schema() -> None:
    assert THINGS.relation == "gold_internal.dim_things"


@pytest.mark.parametrize(
    "name",
    ["thing_key", "_is_deleted", "_content_hash", "_valid_from", "_is_folded", "_is_current"],
)
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
