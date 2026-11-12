import subprocess
from collections.abc import Iterable

from tests.infra.stack import REPO_ROOT, Stack

GOLD_DDL = REPO_ROOT / "docker/analytics-db/initdb/02-gold-model.sql"
GOLD_QUERIES = REPO_ROOT / "analytics/sql/gold"

BUSINESS_VIEWS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)
STORED_TABLES = {
    "dim_customer",
    "dim_product",
    "dim_date",
    "dim_order_status",
    "fact_order_history",
    "fact_order_item_history",
    "current_orders",
    "current_order_items",
    "pending_orders_by_customer",
    *BUSINESS_VIEWS,
}
INTERNAL_TABLES = STORED_TABLES | {"sync_state"}
STAGING_TABLES = {f"stage_{name}" for name in STORED_TABLES}
PUBLIC_VIEWS = (STORED_TABLES - {"pending_orders_by_customer"}) | {"open_orders", "data_freshness"}

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
  AND a.attname IN (
      '_commit_sequence', '_content_hash', '_is_deleted', '_is_folded', '_version_number'
  )
ORDER BY 1;
"""

VERSION_VALUES = "false, 1, NULL, true"
STATUS_KEYS = {"PENDING": 1, "PROCESSING": 2, "REPROCESSING": 3, "COMPLETED": 4, "UNKNOWN": 5}

# Report rows as the gold job writes them, with a null delivery date and product, a tie for first
# place and a customer missing from dim_customer; the current-state rows include deleted ones.
CURRENT_STATE = f"""
INSERT INTO gold_internal.dim_customer (
    customer_key, customer_id, customer_name, customer_address, is_active, _commit_sequence,
    _content_hash, _is_deleted, _valid_from, _is_folded, _version_number, _valid_to, _is_current
)
VALUES
    (9001, 1, 'Ada Lovelace', 'New Street 2', true, 300, 'a2', false, '2026-11-05 08:00',
        {VERSION_VALUES}),
    (9002, 2, 'Grace Hopper', 'Main Street 3', true, 110, 'g1', false, '2026-11-01 08:00',
        {VERSION_VALUES});

INSERT INTO gold_internal.current_orders (
    order_id, status_key, is_open, customer_id, order_date, delivery_date,
    order_date_key, delivery_date_key, _commit_sequence, _changed_at, _is_deleted
)
VALUES
    (101, 1, true, 1, date '2026-11-09', date '2026-11-10', 20261109, 20261110, 501,
        timestamp '2026-11-09 09:00', false),
    (110, 1, true, 4, date '2026-11-09', date '2026-11-14', 20261109, 20261114, 510,
        timestamp '2026-11-09 09:30', true);

INSERT INTO gold_internal.current_order_items (
    order_item_id, order_id, product_id, quantity, _commit_sequence, _changed_at, _is_deleted
)
VALUES
    (1001, 101, 10, 4, 601, timestamp '2026-11-09 09:01', false),
    (1006, 110, 20, 3, 606, timestamp '2026-11-09 09:01', true);

INSERT INTO gold_internal.open_orders_by_delivery_date_and_status
    (delivery_date, status_code, open_orders)
VALUES
    (date '2026-11-10', 'PENDING', 2),
    (date '2026-11-10', 'UNKNOWN', 1),
    (date '2026-11-11', 'REPROCESSING', 1),
    (NULL, 'PENDING', 1);

INSERT INTO gold_internal.top_delivery_dates_by_open_orders (delivery_date, open_orders)
VALUES (date '2026-11-10', 3), (date '2026-11-11', 1), (NULL, 1);

INSERT INTO gold_internal.pending_items_by_product (product_id, pending_items, pending_quantity)
VALUES (10, 2, 10), (30, 1, 8), (NULL, 1, NULL);

INSERT INTO gold_internal.top_customers_by_pending_orders
    (customer_id, customer_name, pending_orders)
VALUES (1, 'Ada Lovelace', 3), (2, 'Grace Hopper', 3), (4, NULL, 1);
"""

REMOVE_CURRENT_STATE = """
DELETE FROM gold_internal.dim_customer WHERE customer_key IN (9001, 9002);
DELETE FROM gold_internal.current_orders WHERE order_id IN (101, 110);
DELETE FROM gold_internal.current_order_items WHERE order_item_id IN (1001, 1006);
DELETE FROM gold_internal.open_orders_by_delivery_date_and_status;
DELETE FROM gold_internal.top_delivery_dates_by_open_orders;
DELETE FROM gold_internal.pending_items_by_product;
DELETE FROM gold_internal.top_customers_by_pending_orders;
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
INSERT INTO gold_internal.fact_order_history (
    order_id, _commit_sequence, _changed_at, _change_op, status_key, _content_hash, _is_deleted,
    _is_folded, _version_number, _valid_to, _is_current
)
VALUES (
    {order_id}, {commit_sequence}, {changed_at}, 'update', {STATUS_KEYS[status]}, '{status}',
    false, {VERSION_VALUES}
){conflict};
"""


def item_change(order_item_id: int, commit_sequence: int, changed_at: str, quantity: int) -> str:
    """Insert one order item history row; changed_at is a SQL timestamp expression, and the
    quantity stands for the content hash."""
    return f"""
INSERT INTO gold_internal.fact_order_item_history (
    order_item_id, _commit_sequence, _changed_at, _change_op, order_id, product_id, quantity,
    _content_hash, _is_deleted, _is_folded, _version_number, _valid_to, _is_current
)
VALUES (
    {order_item_id}, {commit_sequence}, {changed_at}, 'update', 1, 1, {quantity},
    '{quantity}', false, {VERSION_VALUES}
);
"""


def rolled_back(*statements: str) -> str:
    """Wrap statements in a transaction that is rolled back, so a test leaves no rows behind."""
    return "\n".join(["BEGIN;", *statements, "ROLLBACK;"])


FRESHNESS_FIXTURE = f"""
INSERT INTO gold_internal.dim_customer (
    customer_key, customer_id, _commit_sequence, _content_hash, _is_deleted, _valid_from,
    _is_folded, _version_number, _valid_to, _is_current
)
VALUES
    (1, 1, 10, 'a1', false, timestamp '2026-11-08 10:00', {VERSION_VALUES}),
    (2, 1, 20, 'a2', false, timestamp '2026-11-09 08:00', {VERSION_VALUES});
INSERT INTO gold_internal.current_orders (
    order_id, status_key, is_open, _commit_sequence, _changed_at, _is_deleted
)
VALUES
    (1, 1, true, 30, timestamp '2026-11-09 09:00', false),
    (2, 1, true, 40, timestamp '2026-11-09 09:30', false);
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
