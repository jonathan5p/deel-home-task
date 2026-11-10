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
    describe_table,
    describe_view,
    main,
    postgres_string,
    render,
)


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
