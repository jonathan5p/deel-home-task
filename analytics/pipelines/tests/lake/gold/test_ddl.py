import json
import re
from pathlib import Path

import pytest

from analytics.pipelines.tests.lake.gold.ddl_setup import THINGS, stored
from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree
from pipelines.lake.gold.ddl import (
    PUBLIC_VIEWS,
    PublicView,
    UnknownColumnError,
    column_lines,
    column_list,
    describe_table,
    describe_view,
    main,
    postgres_string,
    render,
    staging_statement,
    table_statement,
    view_statement,
)
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.public import PIPELINE_ONLY_COLUMNS
from pipelines.lake.gold.stored import WriteMode

QUESTION_VIEWS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
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


def test_a_view_is_described_without_the_pipeline_only_columns() -> None:
    assert describe_view(THINGS).splitlines() == [
        "COMMENT ON VIEW gold.things IS",
        "    'Things nobody deleted';",
        "COMMENT ON COLUMN gold.things.thing_id IS",
        "    'Thing identifier';",
        "COMMENT ON COLUMN gold.things.label IS",
        "    'The thing''s label';",
    ]


def test_a_view_selects_its_public_columns_from_its_table_with_its_condition() -> None:
    statement = view_statement(PublicView("things", "NOT _is_deleted"), {"things": THINGS})

    assert statement.split(";")[0].splitlines() == [
        "CREATE OR REPLACE VIEW gold.things AS",
        "SELECT",
        "    thing_id,",
        "    label",
        "FROM gold_internal.things",
        "WHERE NOT _is_deleted",
    ]
    assert "COMMENT ON VIEW gold.things IS" in statement


def test_a_view_can_read_another_table_without_a_condition() -> None:
    statement = view_statement(PublicView("things", table="all_things"), {"things": THINGS})

    assert "FROM gold_internal.all_things;\n" in statement


def test_a_merged_table_has_its_keys_as_primary_key() -> None:
    statement = table_statement(stored(WriteMode.MERGE, ("thing_id", "_commit_sequence")))

    assert statement.startswith("CREATE TABLE IF NOT EXISTS gold_internal.things (\n")
    assert "    PRIMARY KEY (thing_id, _commit_sequence)\n);" in statement
    assert "COMMENT ON TABLE gold_internal.things IS" in statement


def test_a_merged_table_keeps_its_unique_columns_unique() -> None:
    statement = table_statement(stored(WriteMode.MERGE, ("thing_id",), unique=("label",)))

    assert "    PRIMARY KEY (thing_id),\n    UNIQUE (label)\n);" in statement


@pytest.mark.parametrize(
    ("dimension", "key"), [("dim_customer", "customer_key"), ("dim_product", "product_key")]
)
def test_a_dimension_refuses_two_versions_with_the_same_surrogate_key(
    dimension: str, key: str
) -> None:
    statement = table_statement(STORED[dimension])

    assert f"    UNIQUE ({key})\n);" in statement


def test_a_history_table_is_partitioned_by_month_with_the_change_time_in_its_key() -> None:
    statement = table_statement(stored(WriteMode.MERGE, ("thing_id",), history=True))

    key = "    PRIMARY KEY (thing_id, _changed_at)\n) PARTITION BY RANGE (_changed_at);"

    assert key in statement
    assert (
        "CREATE TABLE IF NOT EXISTS gold_internal.things_default\n"
        "    PARTITION OF gold_internal.things DEFAULT;"
    ) in statement
    assert "COMMENT ON TABLE gold_internal.things_default IS" in statement


def test_a_staging_table_is_unlogged_and_takes_nulls() -> None:
    merged = staging_statement(stored(WriteMode.MERGE, ("thing_id",)))

    assert merged.startswith("CREATE UNLOGGED TABLE IF NOT EXISTS gold_internal.stage_things (")
    assert "    thing_id bigint,\n" in merged
    assert "    _is_deleted boolean\n);" in merged


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


@pytest.mark.parametrize("name", list(STORED))
def test_every_stored_table_has_its_table_and_staging_table(name: str) -> None:
    ddl = render()

    assert f"CREATE TABLE IF NOT EXISTS gold_internal.{name} (" in ddl
    assert f"CREATE UNLOGGED TABLE IF NOT EXISTS gold_internal.stage_{name} (" in ddl


def test_the_sync_state_records_one_snapshot_per_table() -> None:
    ddl = render()

    assert "CREATE TABLE IF NOT EXISTS gold_internal.sync_state (" in ddl
    assert "    PRIMARY KEY (table_name)\n);" in ddl
    assert "COMMENT ON COLUMN gold_internal.sync_state.snapshot_id IS" in ddl


def test_retention_partitions_and_drops_the_months_of_the_history_tables() -> None:
    ddl = render()
    history = "'fact_order_history', 'fact_order_item_history'"

    assert f"FOREACH fact IN ARRAY ARRAY[{history}] LOOP" in ddl
    assert f"AND parent.relname IN ({history})" in ddl


@pytest.mark.parametrize(
    ("view", "condition"),
    [
        ("dim_customer", "NOT _is_folded AND NOT _is_deleted"),
        ("dim_product", "NOT _is_folded AND NOT _is_deleted"),
        ("current_orders", "NOT _is_deleted"),
        ("current_order_items", "NOT _is_deleted"),
    ],
)
def test_the_public_views_filter_the_stored_version_columns(view: str, condition: str) -> None:
    body = render().split(f"CREATE OR REPLACE VIEW gold.{view} AS", 1)[1].split(";", 1)[0]

    assert body.endswith(f"FROM gold_internal.{view}\nWHERE {condition}")


@pytest.mark.parametrize("view", QUESTION_VIEWS)
def test_the_business_views_count_the_open_current_orders_when_read(view: str) -> None:
    body = render().split(f"CREATE OR REPLACE VIEW gold.{view} AS", 1)[1].split(";", 1)[0]

    assert "FROM gold_internal.current_" in body
    assert "open_order.is_open AND NOT open_order._is_deleted" in body


@pytest.mark.parametrize(
    ("view", "dimension"),
    [("fact_order_history", "dim_customer"), ("fact_order_item_history", "dim_product")],
)
def test_the_history_views_join_the_dimension_version_valid_at_each_change(
    view: str, dimension: str
) -> None:
    body = render().split(f"CREATE OR REPLACE VIEW gold.{view} AS", 1)[1].split(";", 1)[0]

    assert f"FROM gold_internal.{view} AS fact" in body
    assert f"LEFT JOIN gold_internal.{dimension} AS " in body
    assert "WHERE NOT fact._is_folded" in body


def test_the_open_order_indexes_are_partial_and_described() -> None:
    ddl = render()
    open_rows = "WHERE is_open AND NOT _is_deleted;"

    assert f"ON gold_internal.current_orders (delivery_date, status_key) {open_rows}" in ddl
    assert f"ON gold_internal.current_orders (customer_id) {open_rows}" in ddl
    assert "ON gold_internal.current_order_items (order_id) INCLUDE (product_id, quantity)" in ddl
    for index in (
        "current_orders_open_delivery_idx",
        "current_orders_open_customer_idx",
        "current_order_items_live_order_idx",
    ):
        assert f"COMMENT ON INDEX gold_internal.{index} IS" in ddl


def test_no_table_is_transformed_in_postgresql() -> None:
    ddl = render()

    for word in ("JOIN", "GROUP BY", "OVER (", "MERGE", "generate_series"):
        assert word not in ddl.split("CREATE OR REPLACE FUNCTION")[0]


def test_every_gold_relation_with_a_public_view_is_described_as_one() -> None:
    ddl = render()

    for view in PUBLIC_VIEWS:
        assert f"COMMENT ON VIEW gold.{view.name} IS" in ddl
    assert "COMMENT ON VIEW gold.pending_orders_by_customer IS" not in ddl
    assert "COMMENT ON VIEW gold.sync_state IS" not in ddl


def test_the_public_current_orders_keep_the_open_flag_internal() -> None:
    ddl = render()

    assert "COMMENT ON COLUMN gold_internal.current_orders.is_open IS" in ddl
    assert "COMMENT ON COLUMN gold.current_orders.is_open IS" not in ddl
    assert "COMMENT ON COLUMN gold.dim_order_status.is_open IS" in ddl


def test_open_orders_are_the_current_orders_open_and_not_deleted() -> None:
    view = render().split("CREATE OR REPLACE VIEW gold.open_orders AS", 1)[1].split(";", 1)[0]

    assert view.endswith("FROM gold_internal.current_orders\nWHERE is_open AND NOT _is_deleted")


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
