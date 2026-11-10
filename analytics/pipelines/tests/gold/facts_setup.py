from dataclasses import replace

from pipelines.gold.dimensions import load_facts
from pipelines.schemas.definitions import ColumnDefinition, ColumnType
from pipelines.schemas.gold_definitions import GoldTable

ORDERS = load_facts()["fact_order_history"]
ITEMS = load_facts()["fact_order_item_history"]
(STATUS, ORDER_DATE, DELIVERY_DATE) = ORDERS.lookups
(PRODUCT,) = ITEMS.as_of
(CUSTOMER,) = ORDERS.as_of


def without(table: GoldTable, name: str) -> GoldTable:
    return replace(
        table,
        columns=tuple(c for c in table.columns if c.name != name),
        references=tuple(r for r in table.references if r[0] != name),
    )


def with_column(table: GoldTable, name: str) -> GoldTable:
    """The table with one more column that no silver column or lookup fills."""
    return replace(table, columns=(*table.columns, ColumnDefinition(name, ColumnType.LONG, name)))
