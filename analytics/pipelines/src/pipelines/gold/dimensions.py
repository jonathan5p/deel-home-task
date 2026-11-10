from collections.abc import Iterable, Mapping
from importlib.resources.abc import Traversable

from pipelines.gold.scd2 import Scd2Dimension
from pipelines.schemas.definitions import SCHEMAS, EntityDefinition, load_entities
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables


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


def load_dimensions(root: Traversable = SCHEMAS) -> Mapping[str, Scd2Dimension]:
    return scd2_dimensions(load_gold_tables(root), load_entities(root))


DIMENSIONS = tuple(load_dimensions().values())
