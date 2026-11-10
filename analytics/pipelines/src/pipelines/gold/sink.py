from collections.abc import Callable, Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.settings import AnalyticsDbSettings
from pipelines.gold.postgres import ConnectionFactory, run_in_transaction
from pipelines.gold.scd2 import Scd2Dimension

POSTGRES_DRIVER = "org.postgresql.Driver"

type StageWriter = Callable[[DataFrame, str], None]
type BatchWriter = Callable[[DataFrame, int], None]


def staged_rows(silver: DataFrame, dimension: Scd2Dimension) -> DataFrame:
    """The silver rows as the dimension's staged columns, under their gold names."""
    dimension.check_silver(silver.columns)
    return silver.select(
        *(F.col(source).alias(column) for column, source in dimension.silver_columns.items())
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
class Scd2Sink:
    """Writes one micro-batch of a dimension: stages its versions, then merges them in one
    transaction. Both steps are idempotent, so a replayed batch changes nothing."""

    dimension: Scd2Dimension
    stage: StageWriter
    connect: ConnectionFactory

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        self.stage(batch, self.dimension.staging_relation)
        run_in_transaction(self.connect, self.dimension.merge_statements())
