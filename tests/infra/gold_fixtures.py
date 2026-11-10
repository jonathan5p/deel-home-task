import subprocess
from collections.abc import Iterable

from tests.infra.stack import REPO_ROOT, Stack

GOLD_DDL = REPO_ROOT / "docker/analytics-db/initdb/02-gold-model.sql"
GOLD_QUERIES = REPO_ROOT / "analytics/sql/gold"

INTERNAL_TABLES = {
    "dim_customer",
    "dim_product",
    "dim_date",
    "dim_order_status",
    "fact_order_history",
    "fact_order_item_history",
    "current_orders",
    "current_order_items",
}
STAGING_TABLES = {
    "stage_dim_customer",
    "stage_dim_product",
    "stage_fact_order_history",
    "stage_fact_order_item_history",
}
VERSIONS_VIEWS = {"dim_customer_versions", "dim_product_versions"}
BUSINESS_VIEWS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)
PUBLIC_VIEWS = INTERNAL_TABLES | {"data_freshness", *BUSINESS_VIEWS}

UNDESCRIBED_RELATIONS = """
SELECT n.nspname || '.' || c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('gold', 'gold_internal')
  AND c.relkind IN ('r', 'p', 'v')
  AND coalesce(obj_description(c.oid, 'pg_class'), '') = ''
ORDER BY 1;
"""

# Partitions share their parent's columns, so only the parents' columns are checked.
UNDESCRIBED_COLUMNS = """
SELECT n.nspname || '.' || c.relname || '.' || a.attname
FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('gold', 'gold_internal')
  AND c.relkind IN ('r', 'p', 'v')
  AND NOT c.relispartition
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND coalesce(col_description(c.oid, a.attnum), '') = ''
ORDER BY 1;
"""

# Indexes behind a constraint are named and described by it, and partition indexes by their
# parent's index.
UNDESCRIBED_INDEXES = """
SELECT n.nspname || '.' || c.relname
FROM pg_index AS i
JOIN pg_class AS c ON c.oid = i.indexrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('gold', 'gold_internal')
  AND NOT c.relispartition
  AND NOT EXISTS (
      SELECT FROM pg_constraint AS k
      WHERE k.conindid = i.indexrelid AND k.contype IN ('p', 'u', 'x')
  )
  AND coalesce(obj_description(c.oid, 'pg_class'), '') = ''
ORDER BY 1;
"""

CURRENT_STATE_INDEXES = """
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'gold_internal' AND tablename IN ('current_orders', 'current_order_items')
ORDER BY 1;
"""

UNDESCRIBED_SCHEMAS = """
SELECT nspname
FROM pg_namespace
WHERE nspname IN ('gold', 'gold_internal')
  AND coalesce(obj_description(oid, 'pg_namespace'), '') = ''
ORDER BY 1;
"""


def top_level_relations(schema: str) -> str:
    """Tables (r, p) and views (v) of a schema, partitions left out, as name|kind rows."""
    return f"""
SELECT c.relname, c.relkind
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = '{schema}' AND c.relkind IN ('r', 'p', 'v') AND NOT c.relispartition;
"""


def column_types(relation: str) -> str:
    """The columns of a relation in order, as name|type rows."""
    return f"""
SELECT attname, atttypid::regtype::text
FROM pg_attribute
WHERE attrelid = '{relation}'::regclass AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
"""


def type_names(types: Iterable[str]) -> str:
    """One row with the canonical name of each type, as `column_types` shows it."""
    return "SELECT " + ", ".join(f"'{name}'::regtype::text" for name in types) + ";"


PUBLIC_PIPELINE_ONLY_COLUMNS = """
SELECT c.relname || '.' || a.attname
FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'gold'
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attname IN ('_commit_sequence', '_content_hash', '_is_deleted', '_is_folded')
ORDER BY 1;
"""

# Each case is load-bearing for the expected query results: a COMPLETED order (108), a
# deleted order (110), an UNKNOWN status (103), a customer with two versions (1), a customer
# missing from dim_customer (4), and ties for third place in the top-3 questions.
CURRENT_STATE = """
INSERT INTO gold_internal.dim_customer (
    customer_id, customer_name, customer_address, is_active, _commit_sequence,
    _content_hash, _is_deleted, _valid_from
)
VALUES
    (1, 'Ada', 'Old Street 1', true, 100, 'a1', false, '2026-11-01 08:00'),
    (1, 'Ada Lovelace', 'New Street 2', true, 300, 'a2', false, '2026-11-05 08:00'),
    (2, 'Grace Hopper', 'Main Street 3', true, 110, 'g1', false, '2026-11-01 08:00'),
    (3, 'Alan Turing', 'Side Street 4', true, 120, 't1', false, '2026-11-01 08:00');

INSERT INTO gold_internal.current_orders (
    order_id, status_key, customer_id, order_date, delivery_date,
    order_date_key, delivery_date_key, _commit_sequence, _changed_at, _is_deleted
)
SELECT
    o.order_id, s.status_key, o.customer_id, date '2026-11-09', o.delivery_date,
    20261109, to_char(o.delivery_date, 'YYYYMMDD')::int, 400 + o.order_id,
    timestamp '2026-11-09 09:00', o.is_deleted
FROM (
    VALUES
        (101, 'PENDING', 1, date '2026-11-10', false),
        (102, 'PROCESSING', 1, date '2026-11-10', false),
        (103, 'UNKNOWN', 1, date '2026-11-10', false),
        (104, 'PENDING', 2, date '2026-11-10', false),
        (105, 'PENDING', 2, date '2026-11-11', false),
        (106, 'REPROCESSING', 2, date '2026-11-11', false),
        (107, 'PENDING', 3, date '2026-11-12', false),
        (108, 'COMPLETED', 3, date '2026-11-12', false),
        (109, 'PROCESSING', 4, date '2026-11-13', false),
        (110, 'PENDING', 4, date '2026-11-14', true)
) AS o (order_id, status_code, customer_id, delivery_date, is_deleted)
JOIN gold_internal.dim_order_status AS s ON s.status_code = o.status_code;

INSERT INTO gold_internal.current_order_items (
    order_item_id, order_id, product_id, quantity, _commit_sequence, _changed_at, _is_deleted
)
SELECT
    i.order_item_id, i.order_id, i.product_id, i.quantity,
    600 + i.order_item_id, timestamp '2026-11-09 09:01', i.is_deleted
FROM (
    VALUES
        (1001, 101, 10, 4, false),
        (1002, 101, 10, 6, false),
        (1003, 108, 10, 5, false),
        (1004, 110, 20, 7, false),
        (1005, 105, 20, 2, false),
        (1006, 105, 20, 3, true),
        (1007, 999, 30, 1, false),
        (1008, 103, 30, 8, false)
) AS i (order_item_id, order_id, product_id, quantity, is_deleted);
"""


# Two delivery dates and two customers tie for first place with 3 open orders, then one with 2
# and one with 1: ranked with gaps, the tie takes places 1 and 2, so the single third place
# is the one with 2, and the one with 1 (fourth) is left out.
TIES_AT_THE_TOP = """
INSERT INTO gold_internal.current_orders (
    order_id, status_key, customer_id, order_date, delivery_date,
    order_date_key, delivery_date_key, _commit_sequence, _changed_at, _is_deleted
)
SELECT
    o.order_id, s.status_key, o.customer_id, date '2026-11-09', o.delivery_date,
    20261109, to_char(o.delivery_date, 'YYYYMMDD')::int, 800 + o.order_id,
    timestamp '2026-11-09 09:00', false
FROM (
    VALUES
        (201, 11, date '2026-12-01'), (202, 11, date '2026-12-01'), (203, 11, date '2026-12-01'),
        (204, 12, date '2026-12-02'), (205, 12, date '2026-12-02'), (206, 12, date '2026-12-02'),
        (207, 13, date '2026-12-03'), (208, 13, date '2026-12-03'),
        (209, 14, date '2026-12-04')
) AS o (order_id, customer_id, delivery_date)
JOIN gold_internal.dim_order_status AS s ON s.status_code = 'PENDING';
"""


def psql(stack: Stack, script: str) -> subprocess.CompletedProcess[str]:
    """Run a SQL script in the analytics database; stdout holds only the query rows."""
    return subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "analytics-db",
            "psql",
            "--no-psqlrc",
            "--quiet",
            "--no-align",
            "--tuples-only",
            "--set=ON_ERROR_STOP=1",
            "--username=analytics",
            "--dbname=analytics",
            "--file=-",
        ],
        cwd=REPO_ROOT,
        env=stack.env,
        input=script,
        capture_output=True,
        text=True,
        check=False,
    )


def rows(stack: Stack, script: str) -> list[list[str]]:
    run = psql(stack, script)
    assert run.returncode == 0, run.stderr
    return [line.split("|") for line in run.stdout.splitlines()]


def order_change(
    order_id: int,
    commit_sequence: int,
    changed_at: str,
    status: str = "PENDING",
    *,
    ignore_duplicate: bool = False,
) -> str:
    """Insert one order history row; changed_at is a SQL timestamp expression, and the status
    stands for the content hash."""
    conflict = " ON CONFLICT DO NOTHING" if ignore_duplicate else ""
    return f"""
INSERT INTO gold_internal.fact_order_history
    (order_id, _commit_sequence, _changed_at, _change_op, status_key, _content_hash, _is_deleted)
SELECT {order_id}, {commit_sequence}, {changed_at}, 'update', status_key, '{status}', false
FROM gold_internal.dim_order_status
WHERE status_code = '{status}'{conflict};
"""


def item_change(order_item_id: int, commit_sequence: int, changed_at: str, quantity: int) -> str:
    """Insert one order item history row; changed_at is a SQL timestamp expression, and the
    quantity stands for the content hash."""
    return f"""
INSERT INTO gold_internal.fact_order_item_history (
    order_item_id, _commit_sequence, _changed_at, _change_op, order_id, product_id, quantity,
    _content_hash, _is_deleted
)
VALUES (
    {order_item_id}, {commit_sequence}, {changed_at}, 'update', 1, 1, {quantity},
    '{quantity}', false
);
"""


def rolled_back(*statements: str) -> str:
    """Wrap statements in a transaction that is rolled back, so a test leaves no rows behind."""
    return "\n".join(["BEGIN;", *statements, "ROLLBACK;"])


def answer(stack: Stack, query_file: str, fixture: str = CURRENT_STATE) -> list[list[str]]:
    """Run one of the business-question queries on a current-state fixture."""
    query = (GOLD_QUERIES / query_file).read_text()
    return rows(stack, rolled_back(fixture, query))


FRESHNESS_FIXTURE = """
INSERT INTO gold_internal.dim_customer (
    customer_id, _commit_sequence, _content_hash, _is_deleted, _valid_from
)
VALUES
    (1, 10, 'a1', false, timestamp '2026-11-08 10:00'),
    (1, 20, 'a2', false, timestamp '2026-11-09 08:00');
INSERT INTO gold_internal.current_orders (order_id, status_key, _commit_sequence, _changed_at)
VALUES (1, 1, 30, timestamp '2026-11-09 09:00'), (2, 1, 40, timestamp '2026-11-09 09:30');
"""

FRESHNESS = (
    "SELECT entity, coalesce(last_changed_at::text, '') FROM gold.data_freshness ORDER BY entity;"
)

DELETED_ROWS = """
SELECT 'orders', count(*) FILTER (WHERE order_id = 110), count(*)
FROM gold.current_orders
UNION ALL
SELECT 'order_items', count(*) FILTER (WHERE order_item_id = 1006), count(*)
FROM gold.current_order_items;
"""
