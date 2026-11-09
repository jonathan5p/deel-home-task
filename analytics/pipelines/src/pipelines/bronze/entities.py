from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.definitions import ColumnDefinition, EntityDefinition, LogicalType, load_entities

VARIABLE_SCALE_DECIMAL = StructType(
    [StructField("scale", IntegerType()), StructField("value", StringType())]
)

JSON_TYPES: Mapping[LogicalType, DataType] = {
    LogicalType.BIGINT: LongType(),
    LogicalType.INTEGER: IntegerType(),
    LogicalType.VARCHAR: StringType(),
    LogicalType.BOOLEAN: BooleanType(),
    LogicalType.TIMESTAMP: LongType(),
    LogicalType.DATE: IntegerType(),
    LogicalType.DECIMAL: VARIABLE_SCALE_DECIMAL,
}


def json_field(column: ColumnDefinition) -> StructField:
    return StructField(column.name, JSON_TYPES[column.type])


@dataclass(frozen=True, slots=True)
class Entity:
    definition: EntityDefinition

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def key(self) -> str:
        return self.definition.key

    @property
    def columns(self) -> tuple[ColumnDefinition, ...]:
        return self.definition.all_columns

    @property
    def row_schema(self) -> StructType:
        """The row image (`before` / `after`) as Debezium emits it in JSON."""
        return StructType([json_field(column) for column in self.columns])

    @property
    def audit_columns(self) -> tuple[str, ...]:
        return tuple(column.lake_name for column in self.definition.audit_columns)

    @property
    def content_columns(self) -> tuple[str, ...]:
        """Lake names of every column except the key and the audit columns."""
        return tuple(
            column.lake_name for column in self.definition.columns if column.name != self.key
        )

    @property
    def not_null_columns(self) -> tuple[str, ...]:
        """Source names of the columns declared NOT NULL in the source DDL."""
        return tuple(column.name for column in self.columns if column.not_null)


class UnknownEntityError(LookupError):
    def __init__(self, topic: str, known: Iterable[str]) -> None:
        super().__init__(
            f"Topic {topic!r} does not name a known source table; known: {', '.join(known)}"
        )


ENTITIES = tuple(Entity(definition) for definition in load_entities())
_BY_NAME = {entity.name: entity for entity in ENTITIES}
CUSTOMERS = _BY_NAME["customers"]
PRODUCTS = _BY_NAME["products"]
ORDERS = _BY_NAME["orders"]
ORDER_ITEMS = _BY_NAME["order_items"]


def entity_for_topic(topic: str, entities: Iterable[Entity] = ENTITIES) -> Entity:
    """Resolve the entity from a Debezium topic name.

    The last segment of the topic `<prefix>.<schema>.<table>` names the source table, which
    identifies the entity.
    """
    by_table = {entity.name: entity for entity in entities}
    table = topic.rsplit(".", 1)[-1]
    if table not in by_table:
        raise UnknownEntityError(topic, by_table)
    return by_table[table]
