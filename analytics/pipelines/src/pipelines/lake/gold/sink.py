from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.serving import StageWriter
from pipelines.schemas.definitions import EntityDefinition

POSTGRES_DRIVER = "org.postgresql.Driver"
SCHEDULER_POOL = "spark.scheduler.pool"

type BatchWriter = Callable[[DataFrame, int], None]


class GoldLoad(Protocol):
    """The gold tables loaded from the silver table of one source entity."""

    @property
    def name(self) -> str: ...

    @property
    def entity(self) -> EntityDefinition: ...

    @property
    def silver_columns(self) -> Mapping[str, str]: ...

    def check_silver(self, columns: Iterable[str]) -> None: ...


class BatchLoader(Protocol):
    def write_batch(self, load: str, batch: DataFrame, batch_id: int | None = None) -> None: ...


def staged_rows(silver: DataFrame, load: GoldLoad) -> DataFrame:
    """The silver rows as the load's staged columns, under their gold names."""
    load.check_silver(silver.columns)
    return silver.select(
        *(F.col(source).alias(column) for column, source in load.silver_columns.items())
    )


def jdbc_options(settings: AnalyticsDbSettings, table: str) -> Mapping[str, str]:
    """Spark JDBC options that replace the rows of `table` but keep its definition."""
    return {
        "url": settings.url,
        "user": settings.user,
        "password": settings.password,
        "driver": POSTGRES_DRIVER,
        "dbtable": table,
        "truncate": "true",
    }


def jdbc_stage(settings: AnalyticsDbSettings) -> StageWriter:
    def stage(batch: DataFrame, table: str) -> None:
        batch.write.format("jdbc").mode("overwrite").options(**jdbc_options(settings, table)).save()

    return stage


@dataclass(frozen=True, slots=True)
class GoldSink:
    """The foreachBatch function of a load's stream: hands each micro-batch to the writer. The
    batch's jobs run in a scheduler pool named after the load, so under fair scheduling the
    streams share the executors instead of queueing behind each other's jobs."""

    load: GoldLoad
    writer: BatchLoader

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        batch.sparkSession.sparkContext.setLocalProperty(SCHEDULER_POOL, self.load.name)
        self.writer.write_batch(self.load.name, batch, batch_id)
