from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType

from pipelines.lake.streams import TableSink
from pipelines.lake.tables import TableDefinition
from pipelines.schemas.definitions import ColumnDefinition, ColumnType

NUMBERS = TableDefinition(
    comment="Numbers",
    columns={"number": ColumnDefinition("number", ColumnType.LONG, "A number")},
    partitioned_by="number",
)


@dataclass(frozen=True, slots=True)
class ExampleSettings:
    LAYER: ClassVar[str] = "example"

    checkpoint_root: Path
    trigger_seconds: int = 1

    def table(self, name: str) -> str:
        return f"catalog.example.{name}"

    def checkpoint_path(self, name: str) -> str:
        return f"{self.checkpoint_root}/example/{name}"


class FakeCatalog:
    """Records the tables a job creates and writes their streams to Parquet folders."""

    def __init__(
        self, root: Path, order_by: Callable[[Row], tuple[object, ...]] | None = None
    ) -> None:
        self.root = root
        self.order_by = order_by
        self.schemas: dict[str, StructType] = {}
        self.definitions: dict[str, TableDefinition] = {}

    def create(
        self, spark: SparkSession, table: str, schema: StructType, definition: TableDefinition
    ) -> None:
        self.schemas[table] = schema
        self.definitions[table] = definition

    def sink(self) -> TableSink:
        def append(writer: DataStreamWriter, table: str) -> StreamingQuery:
            return writer.format("parquet").start(str(self.root / table))

        return append

    def rows(self, spark: SparkSession, table: str) -> list[Row]:
        rows = spark.read.schema(self.schemas[table]).parquet(str(self.root / table)).collect()
        return rows if self.order_by is None else sorted(rows, key=self.order_by)


def numbers(spark: SparkSession, folder: Path, count: int) -> DataFrame:
    """A file stream over `count` one-row files that reads one file per micro-batch."""
    for number in range(count):
        batch = spark.createDataFrame([(number,)], "number long")
        batch.coalesce(1).write.mode("append").parquet(str(folder))
    return (
        spark.readStream.schema("number long").option("maxFilesPerTrigger", 1).parquet(str(folder))
    )


def failing(rows: DataFrame) -> DataFrame:
    return rows.select(F.raise_error(F.lit("bad record")).alias("failure"))


def start(rows: DataFrame, name: str, tmp_path: Path, *, available_now: bool) -> StreamingQuery:
    """Start a query into a memory table called `name`."""
    writer = (
        rows.writeStream.queryName(name)
        .format("memory")
        .option("checkpointLocation", str(tmp_path / "checkpoints" / name))
    )
    if available_now:
        return writer.trigger(availableNow=True).start()
    return writer.trigger(processingTime="1 second").start()
