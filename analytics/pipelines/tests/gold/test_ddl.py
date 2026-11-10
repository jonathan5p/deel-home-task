import json
import re
from pathlib import Path

import pytest

from analytics.pipelines.tests.gold.ddl_setup import THINGS, VALID_TO
from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree
from pipelines.gold.ddl import (
    PIPELINE_ONLY_COLUMNS,
    UnknownColumnError,
    column_lines,
    column_list,
    describe_table,
    describe_view,
    main,
    postgres_string,
    render,
)
from pipelines.gold.dimensions import DIMENSIONS


def test_a_text_becomes_a_postgres_string_with_its_quotes_doubled() -> None:
    assert postgres_string("the source's key") == "'the source''s key'"


def test_column_lines_carry_the_type_nullability_and_column_constraints() -> None:
    lines = column_lines(THINGS, {"thing_id": "GENERATED ALWAYS AS IDENTITY"})

    assert lines.splitlines() == [
        "    thing_id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,",
        "    label varchar,",
        "    _commit_sequence bigint NOT NULL,",
        "    _is_deleted boolean NOT NULL,",
    ]


def test_a_column_list_has_no_trailing_comma_for_a_table_without_constraints() -> None:
    assert column_list(THINGS).splitlines() == [
        "    thing_id bigint NOT NULL,",
        "    label varchar,",
        "    _commit_sequence bigint NOT NULL,",
        "    _is_deleted boolean NOT NULL",
    ]


def test_a_constraint_on_a_column_the_table_lacks_is_refused() -> None:
    with pytest.raises(UnknownColumnError, match="things has no columns named nope"):
        column_lines(THINGS, {"nope": "UNIQUE"})


def test_a_table_is_described_in_the_internal_schema_with_every_column() -> None:
    assert describe_table(THINGS).splitlines() == [
        "COMMENT ON TABLE gold_internal.things IS",
        "    'Things, one row per thing';",
        "COMMENT ON COLUMN gold_internal.things.thing_id IS",
        "    'Thing identifier';",
        "COMMENT ON COLUMN gold_internal.things.label IS",
        "    'The thing''s label';",
        "COMMENT ON COLUMN gold_internal.things._commit_sequence IS",
        "    'Commit position';",
        "COMMENT ON COLUMN gold_internal.things._is_deleted IS",
        "    'Deleted';",
    ]


def test_a_view_is_described_without_the_pipeline_only_columns_and_with_its_own() -> None:
    assert describe_view(THINGS, VALID_TO).splitlines() == [
        "COMMENT ON VIEW gold.things IS",
        "    'Things nobody deleted';",
        "COMMENT ON COLUMN gold.things.thing_id IS",
        "    'Thing identifier';",
        "COMMENT ON COLUMN gold.things.label IS",
        "    'The thing''s label';",
        "COMMENT ON COLUMN gold.things._valid_to IS",
        "    'Replaced at';",
    ]


def test_the_ddl_takes_its_descriptions_from_the_schemas(schemas: SchemasTree) -> None:
    path = schemas.root / "gold" / "dim_date.avsc"
    dim_date = json.loads(path.read_text(encoding="utf-8"))
    dim_date["fields"][0]["doc"] = "Day as yyyymmdd, the planner's key"
    schemas.write("gold/dim_date.avsc", dim_date)

    ddl = render(schemas.root)

    description = "    'Day as yyyymmdd, the planner''s key';"
    assert f"COMMENT ON COLUMN gold_internal.dim_date.date_key IS\n{description}" in ddl
    assert f"COMMENT ON COLUMN gold.dim_date.date_key IS\n{description}" in ddl


@pytest.mark.parametrize("column", sorted(PIPELINE_ONLY_COLUMNS))
def test_the_public_views_never_describe_a_pipeline_only_column(column: str) -> None:
    ddl = render()

    assert re.search(rf"COMMENT ON COLUMN gold_internal\.\w+\.{column} IS", ddl)
    assert not re.search(rf"COMMENT ON COLUMN gold\.\w+\.{column} IS", ddl)


def test_main_writes_the_rendered_ddl_to_the_given_file(tmp_path: Path) -> None:
    output = tmp_path / "gold.sql"

    main([str(output)])

    assert output.read_text(encoding="utf-8") == render()


@pytest.mark.parametrize("dimension", ["dim_customer", "dim_product"])
def test_each_scd2_dimension_has_an_unlogged_staging_table_described_like_it(
    dimension: str,
) -> None:
    ddl = render()

    assert f"CREATE UNLOGGED TABLE IF NOT EXISTS gold_internal.stage_{dimension} (" in ddl
    assert f"COMMENT ON TABLE gold_internal.stage_{dimension} IS" in ddl
    assert f"COMMENT ON COLUMN gold_internal.stage_{dimension}._valid_from IS" in ddl
    assert f"COMMENT ON COLUMN gold_internal.stage_{dimension}._is_current IS" not in ddl


@pytest.mark.parametrize("dimension", ["dim_customer", "dim_product"])
def test_the_public_dimension_views_leave_out_folded_and_deleted_versions(dimension: str) -> None:
    view = render().split(f"CREATE OR REPLACE VIEW gold.{dimension} AS", 1)[1]

    assert view.split(";", 1)[0].endswith(
        f"FROM gold_internal.{dimension}_versions\nWHERE NOT _is_folded AND NOT _is_deleted"
    )


@pytest.mark.parametrize("dimension", ["dim_customer", "dim_product"])
def test_each_scd2_dimension_has_a_described_versions_view_with_its_query(dimension: str) -> None:
    ddl = render()
    (scd2,) = (d for d in DIMENSIONS if d.name == dimension)

    assert f"CREATE OR REPLACE VIEW gold_internal.{dimension}_versions AS\n" in ddl
    assert f"{scd2.versions_query()};\n" in ddl
    assert f"COMMENT ON VIEW gold_internal.{dimension}_versions IS" in ddl
    assert f"COMMENT ON COLUMN gold_internal.{dimension}_versions._is_folded IS" in ddl


@pytest.mark.parametrize("dimension", ["dim_customer", "dim_product"])
def test_the_dimension_tables_store_no_derived_flags_or_current_constraint(dimension: str) -> None:
    table = render().split(f"CREATE TABLE IF NOT EXISTS gold_internal.{dimension} (", 1)[1]
    definition = table.split(");", 1)[0]

    assert "_is_folded" not in definition
    assert "_is_current" not in definition
    assert "CHECK" not in definition
    assert f"{dimension}_current_idx" not in render()
