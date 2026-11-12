from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.facts import AsOfLookup, ChangeFact
from pipelines.lake.gold.model.columns import IS_REMOVED
from pipelines.lake.gold.model.dimensions import dimension_versions
from pipelines.lake.gold.model.facts import (
    current_rows,
    history_rows,
    merged_current,
    resolved_history,
)
from pipelines.lake.gold.model.reports import (
    PRODUCT_ID,
    open_orders_by_delivery_date_and_status,
    pending_items_by_product,
    pending_orders_by_customer,
    products_of_orders,
    top_customers_by_pending_orders,
    top_delivery_dates_by_open_orders,
)
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, all_of, version_changes
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.stored import (
    OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS,
    PENDING_ITEMS_BY_PRODUCT,
    PENDING_ORDERS_BY_CUSTOMER,
    SEEDED,
    TOP_CUSTOMERS,
    TOP_DELIVERY_DATES,
    StoredTable,
)

ORDER_STATUSES = "dim_order_status"
CUSTOMERS = "dim_customer"
ORDERS = "current_orders"
ORDER_ITEMS = "current_order_items"

type Reader = Callable[[str], DataFrame]


@dataclass(frozen=True, slots=True)
class Write:
    """Rows a batch computed for a stored gold table, written as its mode says."""

    table: StoredTable
    rows: DataFrame


def merged(stored: DataFrame, upserts: DataFrame, keys: Sequence[str]) -> DataFrame:
    """`stored` with the `upserts` merged on `keys`, as the merge leaves the table."""
    kept = stored.join(upserts.select(*keys), list(keys), "left_anti")
    return kept.unionByName(upserts.select(*stored.columns))


def recounted(stored: DataFrame, recount: DataFrame, keys: Sequence[str]) -> DataFrame:
    """A report's `stored` rows with a `recount` applied on its null-safe `keys`, as the merge
    leaves them: the recounted keys replaced, those `_is_removed` deleted."""
    renamed = recount.select(*(F.col(key).alias(f"__recount_{key}") for key in keys))
    matching = all_of([F.col(key).eqNullSafe(F.col(f"__recount_{key}")) for key in keys])
    kept = stored.join(renamed, matching, "left_anti")
    return kept.unionByName(recount.where(~F.col(IS_REMOVED)).select(*stored.columns))


def with_resolved(history: DataFrame, resolved: DataFrame, fact: ChangeFact) -> DataFrame:
    """The stored history rows `resolved` re-resolves, whole, with their new looked-up values."""
    identity = [fact.key, COMMIT_SEQUENCE]
    looked_up = [column for column in resolved.columns if column not in identity]
    return history.drop(*looked_up).join(resolved, identity).select(*history.columns)


class GoldBatches:
    """Computes the writes of one micro-batch of a gold load from its staged rows and the
    stored gold tables, which `read` returns by name.

    A dimension batch writes the versions it adds or re-places, then the history rows of the
    facts whose as-of lookups it changes. A fact batch writes its history and current state,
    then recounts the reports for the keys it touched and ranks the top 3 again. The writes
    come in the order they must be applied: dimensions before facts, current state before
    reports.
    """

    def __init__(
        self,
        tables: Mapping[str, StoredTable],
        dimensions: Iterable[Scd2Dimension],
        facts: Iterable[ChangeFact],
    ) -> None:
        self.tables = tables
        self.dimensions = {dimension.name: dimension for dimension in dimensions}
        self.facts = {fact.name: fact for fact in facts}

    def writes(self, load: str, batch: DataFrame, read: Reader) -> list[Write]:
        if load in self.dimensions:
            return self.dimension_writes(self.dimensions[load], batch, read)
        return self.fact_writes(self.facts[load], batch, read)

    def _write(self, name: str, rows: DataFrame) -> Write:
        return Write(self.tables[name], rows)

    def _lookups_on(self, dimension: Scd2Dimension) -> list[tuple[ChangeFact, AsOfLookup]]:
        return [
            (fact, lookup)
            for fact in self.facts.values()
            for lookup in fact.as_of
            if lookup.dimension.name == dimension.name
        ]

    def dimension_writes(
        self, dimension: Scd2Dimension, batch: DataFrame, read: Reader
    ) -> list[Write]:
        stored = read(dimension.name)
        versions = dimension_versions(dimension, batch, stored)
        changes = version_changes(versions, stored, dimension.key)
        writes = [self._write(dimension.name, changes)]
        for fact, lookup in self._lookups_on(dimension):
            history = read(fact.history.name)
            resolved = resolved_history(fact, lookup, history, versions)
            writes.append(self._write(fact.history.name, with_resolved(history, resolved, fact)))
        if dimension.name == CUSTOMERS:
            customers = merged(stored, changes, (dimension.key, COMMIT_SEQUENCE))
            top = top_customers_by_pending_orders(read(PENDING_ORDERS_BY_CUSTOMER), customers)
            writes.append(self._write(TOP_CUSTOMERS, top))
        return writes

    def fact_writes(self, fact: ChangeFact, batch: DataFrame, read: Reader) -> list[Write]:
        seeded = {name: read(name) for name in SEEDED}
        versions = {lookup.dimension.name: read(lookup.dimension.name) for lookup in fact.as_of}
        stored_history = read(fact.history.name)
        history = history_rows(fact, batch, stored_history, seeded, versions)
        stored_current = read(fact.current.name)
        upserts = current_rows(fact, batch, stored_current, seeded)
        writes = [
            self._write(fact.history.name, version_changes(history, stored_history, fact.key)),
            self._write(fact.current.name, upserts),
        ]
        after = merged_current(stored_current, upserts, fact.key)
        before = stored_current.join(upserts.select(fact.key), fact.key, "left_semi")
        changed = before.unionByName(upserts.select(*before.columns))
        if fact.current.name == ORDERS:
            writes += self._order_reports(changed, after, read, seeded[ORDER_STATUSES])
        elif fact.current.name == ORDER_ITEMS:
            by_product = pending_items_by_product(changed.select(PRODUCT_ID), after, read(ORDERS))
            writes.append(self._write(PENDING_ITEMS_BY_PRODUCT, by_product))
        return writes

    def _order_reports(
        self, changed: DataFrame, orders: DataFrame, read: Reader, statuses: DataFrame
    ) -> list[Write]:
        by_date = open_orders_by_delivery_date_and_status(changed, orders, statuses)
        by_customer = pending_orders_by_customer(changed, orders)
        items = read(ORDER_ITEMS)
        by_product = pending_items_by_product(products_of_orders(changed, items), items, orders)
        dates = self._recounted(OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS, by_date, read)
        customers = self._recounted(PENDING_ORDERS_BY_CUSTOMER, by_customer, read)
        return [
            self._write(OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS, by_date),
            self._write(PENDING_ORDERS_BY_CUSTOMER, by_customer),
            self._write(PENDING_ITEMS_BY_PRODUCT, by_product),
            self._write(TOP_DELIVERY_DATES, top_delivery_dates_by_open_orders(dates)),
            self._write(TOP_CUSTOMERS, top_customers_by_pending_orders(customers, read(CUSTOMERS))),
        ]

    def _recounted(self, name: str, recount: DataFrame, read: Reader) -> DataFrame:
        return recounted(read(name), recount, self.tables[name].keys)
