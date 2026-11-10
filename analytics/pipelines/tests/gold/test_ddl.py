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
from pipelines.schemas.gold_definitions import load_gold_tables

QUESTION_VIEWS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)
VIEWS_WITHOUT_TABLE = ("open_orders", *QUESTION_VIEWS)


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


@pytest.mark.parametrize("fact", ["fact_order_history", "fact_order_item_history"])
def test_each_history_fact_has_an_unlogged_staging_table_with_the_lookup_inputs(
    fact: str,
) -> None:
    ddl = render()

    assert f"CREATE UNLOGGED TABLE IF NOT EXISTS gold_internal.stage_{fact} (" in ddl
    assert f"COMMENT ON TABLE gold_internal.stage_{fact} IS" in ddl
    assert f"COMMENT ON COLUMN gold_internal.stage_{fact}._content_hash IS" in ddl
    assert f"COMMENT ON COLUMN gold_internal.stage_{fact}._valid_to IS" not in ddl


def test_the_order_status_is_staged_with_its_source_description() -> None:
    assert (
        "COMMENT ON COLUMN gold_internal.stage_fact_order_history.status IS\n"
        "    'Order status as recorded in the source, for example PENDING or COMPLETED';"
    ) in render()


@pytest.mark.parametrize(
    ("fact", "key"),
    [("fact_order_history", "order_id"), ("fact_order_item_history", "order_item_id")],
)
def test_the_public_history_views_fold_unchanged_changes_at_read_time(fact: str, key: str) -> None:
    view = render().split(f"CREATE OR REPLACE VIEW gold.{fact} AS", 1)[1].split(";", 1)[0]

    assert "(_content_hash, _is_deleted) IS NOT DISTINCT FROM" in view
    assert "WHERE NOT is_folded\n" in view
    assert "lead(_changed_at) OVER versions IS NULL AND NOT _is_deleted AS _is_current" in view
    assert f"WINDOW versions AS (PARTITION BY {key} ORDER BY _commit_sequence)" in view


@pytest.mark.parametrize(
    ("index", "column"),
    [
        ("fact_order_history_customer_idx", "fact_order_history (customer_id)"),
        ("fact_order_item_history_product_idx", "fact_order_item_history (product_id)"),
    ],
)
def test_history_facts_are_indexed_by_the_dimension_key_they_are_re_resolved_by(
    index: str, column: str
) -> None:
    ddl = render()

    assert f"CREATE INDEX IF NOT EXISTS {index}\n    ON gold_internal.{column};" in ddl
    assert f"COMMENT ON INDEX gold_internal.{index} IS" in ddl


@pytest.mark.parametrize(
    ("index", "definition"),
    [
        (
            "current_orders_open_delivery_idx",
            "current_orders (delivery_date, status_key) WHERE is_open AND NOT _is_deleted",
        ),
        (
            "current_orders_open_customer_idx",
            "current_orders (customer_id) WHERE is_open AND NOT _is_deleted",
        ),
        (
            "current_order_items_live_order_idx",
            "current_order_items (order_id) INCLUDE (product_id, quantity)\n"
            "    WHERE NOT _is_deleted",
        ),
    ],
)
def test_the_current_state_is_indexed_for_the_business_views(index: str, definition: str) -> None:
    ddl = render()

    assert f"CREATE INDEX IF NOT EXISTS {index}\n    ON gold_internal.{definition};" in ddl
    assert f"COMMENT ON INDEX gold_internal.{index} IS" in ddl


def test_every_index_the_ddl_creates_is_described() -> None:
    ddl = render()
    created = re.findall(r"CREATE (?:UNIQUE )?INDEX IF NOT EXISTS (\w+)", ddl)

    assert created
    described = set(re.findall(r"COMMENT ON INDEX gold_internal\.(\w+) IS", ddl))
    assert set(created) <= described


def test_every_gold_relation_but_the_versions_views_is_described_as_a_public_view() -> None:
    ddl = render()
    versions_views = {dimension.versions_relation.split(".")[1] for dimension in DIMENSIONS}

    for name in set(load_gold_tables()) - versions_views:
        assert f"COMMENT ON VIEW gold.{name} IS" in ddl
    for name in versions_views:
        assert f"COMMENT ON VIEW gold.{name} IS" not in ddl


@pytest.mark.parametrize("view", VIEWS_WITHOUT_TABLE)
def test_a_business_view_has_no_internal_table(view: str) -> None:
    ddl = render()

    assert f"CREATE OR REPLACE VIEW gold.{view} AS" in ddl
    assert f"gold_internal.{view}" not in ddl


def test_the_public_current_orders_keep_the_open_flag_internal() -> None:
    ddl = render()

    assert "COMMENT ON COLUMN gold_internal.current_orders.is_open IS" in ddl
    assert "COMMENT ON COLUMN gold.current_orders.is_open IS" not in ddl
    assert "COMMENT ON COLUMN gold.dim_order_status.is_open IS" in ddl


def test_open_orders_are_the_current_orders_open_and_not_deleted() -> None:
    view = render().split("CREATE OR REPLACE VIEW gold.open_orders AS", 1)[1].split(";", 1)[0]

    assert view.endswith("FROM gold_internal.current_orders\nWHERE is_open AND NOT _is_deleted")


@pytest.mark.parametrize("view", QUESTION_VIEWS)
def test_the_business_views_count_the_open_orders(view: str) -> None:
    body = render().split(f"CREATE OR REPLACE VIEW gold.{view} AS", 1)[1].split(";", 1)[0]

    assert "FROM gold.open_orders AS o" in body or "JOIN gold.open_orders AS o" in body
    assert "gold.current_orders" not in body


def test_a_view_cannot_hide_a_column_its_table_lacks() -> None:
    with pytest.raises(UnknownColumnError, match="things has no columns named nope"):
        describe_view(THINGS, hidden=["nope"])


def test_a_view_leaves_out_the_columns_it_hides() -> None:
    lines = describe_view(THINGS, hidden=["label"]).splitlines()

    assert "COMMENT ON COLUMN gold.things.label IS" not in lines
    assert "COMMENT ON COLUMN gold.things.thing_id IS" in lines


def test_the_gold_reader_role_is_created_only_when_missing() -> None:
    ddl = render()

    assert "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'gold_reader') THEN" in ddl
    assert "CREATE ROLE gold_reader NOLOGIN;" in ddl
    assert "COMMENT ON ROLE gold_reader IS" in ddl


def test_the_gold_reader_role_reads_every_public_view_after_they_are_created() -> None:
    ddl = render()
    grants = ddl.index("GRANT SELECT ON ALL TABLES IN SCHEMA gold TO gold_reader;")

    assert "GRANT USAGE ON SCHEMA gold TO gold_reader;" in ddl
    assert grants > ddl.rindex("CREATE OR REPLACE VIEW gold.")
    assert grants < ddl.rindex("COMMIT;")


def test_the_gold_reader_role_gets_nothing_in_the_internal_schema() -> None:
    assert not re.search(r"GRANT [^;]*gold_internal", render())
