from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.lake.silver.job_setup import local, topic
from pipelines.lake.bronze.entities import CUSTOMERS
from pipelines.lake.shared.checkpoints import CommittedBatch
from pipelines.lake.shared.snapshots import Snapshot, table_snapshots
from pipelines.lake.silver.quality import IDENTITY, Committed, SnapshotReader
from pipelines.settings import SilverSettings

TOPIC = topic(CUSTOMERS)

type Change = tuple[int, int, str]


def changes(spark: SparkSession, *rows: Change) -> DataFrame:
    """Change identities: (key, commit sequence, change op)."""
    return spark.createDataFrame(list(rows), IDENTITY)


def committed(
    spark: SparkSession, *, bronze: tuple[Change, ...], silver: tuple[Change, ...]
) -> Committed:
    """A silver stream that committed batch 0, after reading `bronze` and writing `silver`."""
    return Committed(
        expected=changes(spark, *bronze),
        actual=changes(spark, *silver),
        batch=CommittedBatch(query_id="query", batch=0, sources=()),
        bronze_snapshot=1,
        silver_snapshot=2,
    )


def without_snapshots_of(table: str) -> SnapshotReader:
    """Reads the snapshots of every table but `table`, as if all of its had expired."""

    def read(spark: SparkSession, name: str) -> list[Snapshot]:
        return [] if name == table else table_snapshots(spark, name)

    return read


def uncommit_last_batch(settings: SilverSettings) -> None:
    """Forget the commit of the stream's last batch, as if it was written but never committed."""
    commits = local(settings.checkpoint_path(CUSTOMERS.name)) / "commits"
    last = max((path for path in commits.iterdir() if path.name.isdecimal()), key=_number)
    last.unlink()
    (commits / f".{last.name}.crc").unlink(missing_ok=True)


def _number(path: Path) -> int:
    return int(path.name)
