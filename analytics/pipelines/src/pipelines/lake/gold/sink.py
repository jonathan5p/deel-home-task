from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.facts import ChangeFact
from pipelines.lake.gold.postgres import WRITER_LOCK, ConnectionFactory, run_in_transaction
from pipelines.schemas.definitions import EntityDefinition

POSTGRES_DRIVER = "org.postgresql.Driver"

type StageWriter = Callable[[DataFrame, str], None]
type BatchWriter = Callable[[DataFrame, int], None]


class GoldLoad(Protocol):
    """A gold table, or a pair of them, loaded from the silver table of one source entity."""

    @property
    def name(self) -> str: ...

    @property
    def entity(self) -> EntityDefinition: ...

    @property
    def staging_relation(self) -> str: ...

    @property
    def silver_columns(self) -> Mapping[str, str]: ...

    def check_silver(self, columns: Iterable[str]) -> None: ...

    def merge_statements(self) -> Sequence[str]: ...


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
class MergeSink:
    """Writes one micro-batch of a load: stages its rows, then merges them in one transaction,
    followed by the re-resolution of the facts that look up what it wrote. Both steps are
    idempotent, so a replayed batch changes nothing."""

    load: GoldLoad
    stage: StageWriter
    connect: ConnectionFactory
    facts: tuple[ChangeFact, ...] = ()

    def statements(self) -> tuple[str, ...]:
        resolved = (s for fact in self.facts for s in fact.resolve_statements(self.load.name))
        return (WRITER_LOCK, *self.load.merge_statements(), *resolved)

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        self.stage(batch, self.load.staging_relation)
        run_in_transaction(self.connect, self.statements())
