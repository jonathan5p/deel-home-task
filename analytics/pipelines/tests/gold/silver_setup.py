from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.bronze.tables import SPARK_TYPES
from pipelines.gold.job import SilverReader
from pipelines.gold.scd2 import Scd2Dimension
from pipelines.gold.sink import BatchWriter
from pipelines.schemas.definitions import EntityDefinition

CHANGED_AT = datetime(2026, 11, 8, 14, 21)
SILVER_METADATA = StructType(
    [
        StructField("_commit_sequence", LongType()),
        StructField("_change_op", StringType()),
        StructField("_changed_at", TimestampType()),
        StructField("_content_hash", StringType()),
        StructField("_is_deleted", BooleanType()),
    ]
)


def silver_schema(entity: EntityDefinition) -> StructType:
    """The silver table of `entity`: its source and audit columns, then the change metadata."""
    columns = [StructField(c.lake_name, SPARK_TYPES[c.type]) for c in entity.all_columns]
    return StructType([*columns, *SILVER_METADATA])


def silver_version(entity: EntityDefinition, key: int, sequence: int, **values: object) -> Row:
    """One silver version of a row: its key, the given columns, and the change metadata."""
    row = dict.fromkeys(silver_schema(entity).fieldNames())
    metadata = {
        "_commit_sequence": sequence,
        "_change_op": "update",
        "_changed_at": CHANGED_AT,
        "_content_hash": f"hash-{key}-{sequence}",
        "_is_deleted": False,
    }
    return Row(**(row | {entity.key: key} | metadata | values))


PRODUCT_VALUES = {"product_name": "Notebook", "unity_price": Decimal("1.5300000000")}


def write_silver(
    spark: SparkSession, folder: Path, entity: EntityDefinition, rows: list[Row]
) -> None:
    spark.createDataFrame(rows, silver_schema(entity)).write.mode("append").parquet(str(folder))


def silver_folders(root: Path, entities: list[EntityDefinition]) -> SilverReader:
    """A reader that streams each silver table from a parquet folder per entity under `root`."""
    schemas = {entity.name: silver_schema(entity) for entity in entities}

    def read(spark: SparkSession, table: str) -> DataFrame:
        entity = table.rsplit(".", 1)[-1]
        return spark.readStream.schema(schemas[entity]).parquet(str(root / entity))

    return read


class RecordingSinks:
    """A sink factory that keeps every micro-batch it is given, as rows, by dimension."""

    def __init__(self) -> None:
        self.batches: dict[str, list[list[Row]]] = {}

    def __call__(self, dimension: Scd2Dimension) -> BatchWriter:
        def write(batch: DataFrame, batch_id: int) -> None:
            self.batches.setdefault(dimension.name, []).append(batch.collect())

        return write
