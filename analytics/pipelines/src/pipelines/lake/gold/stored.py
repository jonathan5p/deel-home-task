from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from pipelines.lake.gold.facts import ChangeFact, FactColumn
from pipelines.lake.gold.model.columns import IS_REMOVED
from pipelines.lake.gold.scd2 import INTERNAL_SCHEMA, Scd2Dimension
from pipelines.schemas.definitions import ColumnDefinition, ColumnType
from pipelines.schemas.gold_definitions import GoldTable

COMMIT_SEQUENCE = str(FactColumn.COMMIT_SEQUENCE)
STAGING_PREFIX = "stage_"
STAGING_DESCRIPTION = (
    "Rows of the micro-batch being copied into {table}, loaded by Spark and replaced by every"
    " batch; unlogged, so it may be empty after a crash"
)
REMOVED_COLUMN = ColumnDefinition(
    name=IS_REMOVED,
    type=ColumnType.BOOLEAN,
    description="Whether the row's total dropped to zero, so the copy deletes it",
    not_null=True,
)
SEEDED = ("dim_order_status", "dim_date")
SEED_KEYS: Mapping[str, str] = {"dim_order_status": "status_key", "dim_date": "date_key"}
OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS = "open_orders_by_delivery_date_and_status"
PENDING_ORDERS_BY_CUSTOMER = "pending_orders_by_customer"
PENDING_ITEMS_BY_PRODUCT = "pending_items_by_product"
TOP_DELIVERY_DATES = "top_delivery_dates_by_open_orders"
TOP_CUSTOMERS = "top_customers_by_pending_orders"
REPORT_KEYS: Mapping[str, tuple[str, ...]] = {
    OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS: ("delivery_date", "status_code"),
    PENDING_ORDERS_BY_CUSTOMER: ("customer_id",),
    PENDING_ITEMS_BY_PRODUCT: ("product_id",),
}
TOPS = (TOP_DELIVERY_DATES, TOP_CUSTOMERS)


class WriteMode(StrEnum):
    """How the rows a batch computes for a table are written to it."""

    MERGE = "merge"
    """Matched on the keys: every column updated; new rows inserted."""
    MERGE_NEWER = "merge_newer"
    """As MERGE, but a matched row is updated only by a higher `_commit_sequence`."""
    RECOUNT = "recount"
    """Matched null-safe on the keys: rows marked `_is_removed` deleted, the others upserted."""
    REPLACE = "replace"
    """Every row of the table replaced."""


@dataclass(frozen=True, slots=True)
class StoredTable:
    """A gold table the gold job writes to Iceberg and copies into gold_internal in PostgreSQL.
    Its staging table takes nulls in every column: a removed report row has no totals.

    `history` tables are partitioned by month of `_changed_at` in PostgreSQL, where retention
    drops old months, and bucketed by their first key in Iceberg, which keeps every change.
    `unique` is a column PostgreSQL keeps unique besides the key, such as a hashed surrogate
    key, so a hash collision is refused instead of mixing two rows.
    """

    table: GoldTable
    mode: WriteMode
    keys: tuple[str, ...] = ()
    history: bool = False
    unique: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.table.columns)

    @property
    def relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.name}"

    @property
    def postgres_key(self) -> tuple[str, ...]:
        """The PostgreSQL primary key, or unique key of a recounted table; a history table's
        holds `_changed_at` because PostgreSQL requires the partition column in it."""
        if self.history:
            return (*self.keys, str(FactColumn.CHANGED_AT))
        return self.keys

    @property
    def staging_relation(self) -> str:
        return f"{INTERNAL_SCHEMA}.{self.staging_table.name}"

    @property
    def staging_table(self) -> GoldTable:
        removed = (REMOVED_COLUMN,) if self.mode is WriteMode.RECOUNT else ()
        columns = (*self.table.columns, *removed)
        return GoldTable(
            name=f"{STAGING_PREFIX}{self.name}",
            description=STAGING_DESCRIPTION.format(table=self.relation),
            columns=tuple(replace(column, not_null=False) for column in columns),
        )


def stored_tables(
    tables: Mapping[str, GoldTable],
    dimensions: Iterable[Scd2Dimension],
    facts: Iterable[ChangeFact],
) -> Mapping[str, StoredTable]:
    """Every stored gold table by name, in the order a batch writes them: the seeded and SCD2
    dimensions before the facts, current state before the reports."""
    stored = [StoredTable(tables[name], WriteMode.MERGE, (SEED_KEYS[name],)) for name in SEEDED]
    stored += [
        StoredTable(
            dimension.table,
            WriteMode.MERGE,
            (dimension.key, COMMIT_SEQUENCE),
            unique=(dimension.surrogate_key,),
        )
        for dimension in dimensions
    ]
    facts = tuple(facts)
    stored += [
        StoredTable(fact.history, WriteMode.MERGE, (fact.key, COMMIT_SEQUENCE), history=True)
        for fact in facts
    ]
    stored += [StoredTable(fact.current, WriteMode.MERGE_NEWER, (fact.key,)) for fact in facts]
    stored += [
        StoredTable(tables[name], WriteMode.RECOUNT, keys) for name, keys in REPORT_KEYS.items()
    ]
    stored += [StoredTable(tables[name], WriteMode.REPLACE) for name in TOPS]
    return {table.name: table for table in stored}
