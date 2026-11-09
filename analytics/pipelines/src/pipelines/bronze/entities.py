from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum

from pyspark.sql.types import (
    BooleanType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

AUDIT_COLUMNS = ("updated_at", "updated_by", "created_at", "created_by")


class SourceType(StrEnum):
    BIGINT = "bigint"
    INTEGER = "integer"
    VARCHAR = "varchar"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    DECIMAL = "decimal"


VARIABLE_SCALE_DECIMAL = StructType(
    [StructField("scale", IntegerType()), StructField("value", StringType())]
)

JSON_TYPES: Mapping[SourceType, DataType] = {
    SourceType.BIGINT: LongType(),
    SourceType.INTEGER: IntegerType(),
    SourceType.VARCHAR: StringType(),
    SourceType.BOOLEAN: BooleanType(),
    SourceType.TIMESTAMP: LongType(),
    SourceType.DATE: IntegerType(),
    SourceType.DECIMAL: VARIABLE_SCALE_DECIMAL,
}


@dataclass(frozen=True, slots=True)
class SourceColumn:
    name: str
    type: SourceType
    not_null: bool = False
    renamed_to: str | None = None

    @property
    def bronze_name(self) -> str:
        return self.renamed_to or self.name

    @property
    def json_field(self) -> StructField:
        return StructField(self.name, JSON_TYPES[self.type])


@dataclass(frozen=True, slots=True)
class Entity:
    name: str
    key: str
    columns: tuple[SourceColumn, ...]

    def __post_init__(self) -> None:
        if self.key not in {column.name for column in self.columns}:
            raise ValueError(f"Key column {self.key!r} is not a column of entity {self.name!r}")

    @property
    def row_schema(self) -> StructType:
        """The row image (`before` / `after`) as Debezium emits it in JSON."""
        return StructType([column.json_field for column in self.columns])

    @property
    def audit_columns(self) -> tuple[str, ...]:
        return tuple(c.bronze_name for c in self.columns if c.name in AUDIT_COLUMNS)

    @property
    def content_columns(self) -> tuple[str, ...]:
        """Bronze names of every column except the key and the audit columns."""
        excluded = {self.key, *AUDIT_COLUMNS}
        return tuple(c.bronze_name for c in self.columns if c.name not in excluded)

    @property
    def not_null_columns(self) -> tuple[str, ...]:
        """Source names of the columns declared NOT NULL in the source DDL."""
        return tuple(c.name for c in self.columns if c.not_null)


class UnknownEntityError(LookupError):
    def __init__(self, topic: str, known: Iterable[str]) -> None:
        super().__init__(
            f"Topic {topic!r} does not name a known source table; known: {', '.join(known)}"
        )


AUDIT = (
    SourceColumn("updated_at", SourceType.TIMESTAMP),
    SourceColumn("updated_by", SourceType.BIGINT),
    SourceColumn("created_at", SourceType.TIMESTAMP),
    SourceColumn("created_by", SourceType.BIGINT),
)

CUSTOMERS = Entity(
    name="customers",
    key="customer_id",
    columns=(
        SourceColumn("customer_id", SourceType.BIGINT, not_null=True),
        SourceColumn("customer_name", SourceType.VARCHAR, not_null=True),
        SourceColumn("is_active", SourceType.BOOLEAN, not_null=True),
        SourceColumn("customer_address", SourceType.VARCHAR),
        *AUDIT,
    ),
)

PRODUCTS = Entity(
    name="products",
    key="product_id",
    columns=(
        SourceColumn("product_id", SourceType.BIGINT, not_null=True),
        SourceColumn("product_name", SourceType.VARCHAR, not_null=True),
        SourceColumn("barcode", SourceType.VARCHAR, not_null=True),
        SourceColumn("unity_price", SourceType.DECIMAL, not_null=True),
        SourceColumn("is_active", SourceType.BOOLEAN),
        *AUDIT,
    ),
)

ORDERS = Entity(
    name="orders",
    key="order_id",
    columns=(
        SourceColumn("order_id", SourceType.BIGINT, not_null=True),
        SourceColumn("order_date", SourceType.DATE),
        SourceColumn("delivery_date", SourceType.DATE),
        SourceColumn("customer_id", SourceType.BIGINT),
        SourceColumn("status", SourceType.VARCHAR),
        *AUDIT,
    ),
)

ORDER_ITEMS = Entity(
    name="order_items",
    key="order_item_id",
    columns=(
        SourceColumn("order_item_id", SourceType.BIGINT, not_null=True),
        SourceColumn("order_id", SourceType.BIGINT),
        SourceColumn("product_id", SourceType.BIGINT),
        SourceColumn("quanity", SourceType.INTEGER, renamed_to="quantity"),
        *AUDIT,
    ),
)

ENTITIES = (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS)


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
