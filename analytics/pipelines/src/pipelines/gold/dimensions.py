from collections.abc import Iterable, Mapping
from importlib.resources.abc import Traversable

from pipelines.gold.facts import AsOfLookup, ChangeFact, KeyLookup, Multiplied
from pipelines.gold.scd2 import Scd2Dimension
from pipelines.schemas.definitions import SCHEMAS, EntityDefinition, load_entities
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables

UNKNOWN_STATUS = "UNKNOWN"


def scd2_dimensions(
    tables: Mapping[str, GoldTable], entities: Iterable[EntityDefinition]
) -> Mapping[str, Scd2Dimension]:
    """The SCD2 dimensions by gold table, each with the source entity it is loaded from."""
    by_name = {entity.name: entity for entity in entities}
    return {
        "dim_customer": Scd2Dimension(
            tables["dim_customer"], by_name["customers"], surrogate_key="customer_key"
        ),
        "dim_product": Scd2Dimension(
            tables["dim_product"],
            by_name["products"],
            surrogate_key="product_key",
            renamed=(("unit_price", "unity_price"),),
        ),
    }


def change_facts(
    tables: Mapping[str, GoldTable],
    entities: Iterable[EntityDefinition],
    dimensions: Mapping[str, Scd2Dimension],
) -> Mapping[str, ChangeFact]:
    """The history facts by gold table, each with its current-state table, its source entity,
    and the dimensions its columns are looked up in. The as-of lookups also declare which
    facts a dimension's merge re-resolves."""
    by_name = {entity.name: entity for entity in entities}

    def day(column: str, source: str) -> KeyLookup:
        return KeyLookup(column, source, tables["dim_date"], code="calendar_date", key="date_key")

    def status(column: str) -> KeyLookup:
        """The dim_order_status column of the same name for the order's status."""
        return KeyLookup(
            column,
            "status",
            tables["dim_order_status"],
            code="status_code",
            key=column,
            fallback=UNKNOWN_STATUS,
        )

    customer = AsOfLookup(
        dimensions["dim_customer"],
        business_key="customer_id",
        columns=(("customer_key", "customer_key"),),
    )
    product = AsOfLookup(
        dimensions["dim_product"],
        business_key="product_id",
        columns=(("product_key", "product_key"), ("unit_price", "unit_price")),
        multiplied=(Multiplied("line_amount", factor="quantity", looked_up="unit_price"),),
    )
    return {
        "fact_order_history": ChangeFact(
            tables["fact_order_history"],
            tables["current_orders"],
            by_name["orders"],
            lookups=(
                status("status_key"),
                # Stored with the current state so the partial indexes of the business views can
                # filter on it; the planner cannot see through a join how few orders are open.
                status("is_open"),
                day("order_date_key", "order_date"),
                day("delivery_date_key", "delivery_date"),
            ),
            as_of=(customer,),
        ),
        "fact_order_item_history": ChangeFact(
            tables["fact_order_item_history"],
            tables["current_order_items"],
            by_name["order_items"],
            as_of=(product,),
        ),
    }


def load_dimensions(root: Traversable = SCHEMAS) -> Mapping[str, Scd2Dimension]:
    return scd2_dimensions(load_gold_tables(root), load_entities(root))


def load_facts(root: Traversable = SCHEMAS) -> Mapping[str, ChangeFact]:
    tables, entities = load_gold_tables(root), load_entities(root)
    return change_facts(tables, entities, scd2_dimensions(tables, entities))


DIMENSIONS = tuple(load_dimensions().values())
FACTS = tuple(load_facts().values())
