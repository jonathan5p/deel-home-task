from collections.abc import Iterable, Mapping
from importlib.resources.abc import Traversable

from pipelines.lake.gold.facts import ChangeFact, KeyLookup
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.stored import StoredTable, stored_tables
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
) -> Mapping[str, ChangeFact]:
    """The history facts by gold table, each with its current-state table as they are stored,
    its source entity, and the seeded dimensions its keys are looked up in."""
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

    return {
        "fact_order_history": ChangeFact(
            tables["fact_order_history"].stored(),
            tables["current_orders"],
            by_name["orders"],
            lookups=(
                status("status_key"),
                status("is_open"),
                day("order_date_key", "order_date"),
                day("delivery_date_key", "delivery_date"),
            ),
        ),
        "fact_order_item_history": ChangeFact(
            tables["fact_order_item_history"].stored(),
            tables["current_order_items"],
            by_name["order_items"],
        ),
    }


def load_dimensions(root: Traversable = SCHEMAS) -> Mapping[str, Scd2Dimension]:
    return scd2_dimensions(load_gold_tables(root), load_entities(root))


def load_facts(root: Traversable = SCHEMAS) -> Mapping[str, ChangeFact]:
    tables, entities = load_gold_tables(root), load_entities(root)
    return change_facts(tables, entities)


def load_stored(root: Traversable = SCHEMAS) -> Mapping[str, StoredTable]:
    """The gold tables the gold job writes, by name."""
    tables, entities = load_gold_tables(root), load_entities(root)
    dimensions = scd2_dimensions(tables, entities)
    facts = change_facts(tables, entities)
    return stored_tables(tables, dimensions.values(), facts.values())


DIMENSIONS = tuple(load_dimensions().values())
FACTS = tuple(load_facts().values())
STORED = load_stored()
