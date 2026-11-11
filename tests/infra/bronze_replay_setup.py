from dataclasses import dataclass

from tests.infra.bronze_setup import (
    FIRST_LSN,
    TOPICS,
    Json,
    Rows,
    change,
    envelope,
    landing_staged,
    offset,
    record,
    source_row,
)
from tests.infra.lake_reader import duckdb_rows
from tests.infra.landing_avro import landing_avro
from tests.infra.stack import Stack

LAKE_KEY = "test-fixtures/bronze-replay"
STAGING_KEY = f"{LAKE_KEY}/staging"
BRONZE_APP = "analytics/pipelines/src/pipelines/bronze/job.py"
REPLAY_APP = "analytics/pipelines/src/pipelines/bronze/replay.py"
ENTITY = "customers"
TOPIC = TOPICS[ENTITY]

# Its own Iceberg catalog keeps these tables apart from the ones the bronze tests check.
CATALOG = "replay"
CONF = {
    f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG}.type": "hadoop",
    f"spark.sql.catalog.{CATALOG}.warehouse": f"s3a://lake/{LAKE_KEY}/warehouse",
}
ENV = {
    "KAFKA_TOPICS": TOPIC,
    "LAKE_ROOT": f"s3a://lake/{LAKE_KEY}",
    "CHECKPOINT_ROOT": f"s3a://lake/{LAKE_KEY}/checkpoints",
    "ICEBERG_CATALOG": CATALOG,
}
TABLES_PATH = f"s3://lake/{LAKE_KEY}/warehouse/bronze"
BRONZE = f"{TABLES_PATH}/{ENTITY}"
QUARANTINE = f"{TABLES_PATH}/{ENTITY}_quarantine"


@dataclass(frozen=True, slots=True)
class Table:
    rows: Rows

    @property
    def offsets(self) -> list[int]:
        return sorted(offset(row) for row in self.rows)


@dataclass(frozen=True, slots=True)
class ReplayRuns:
    streamed: tuple[Table, Table]
    replayed_ingested: tuple[Table, Table]
    replayed_missing: tuple[Table, Table]
    replayed_again: tuple[Table, Table]
    summaries: list[str]


def submit(stack: Stack, app: str, *args: str) -> str:
    returncode, output = stack.finish(stack.start_app(app, *args, conf=CONF, env=ENV))
    assert returncode == 0, output
    return output


def stage(stack: Stack, batch: str, records: list[Json]) -> None:
    """Stage records as Avro files, for the next run to land first."""
    stack.put_object(f"{STAGING_KEY}/{TOPIC}/{batch}.avro", landing_avro(records))


def tables(stack: Stack) -> tuple[Table, Table]:
    """The bronze and quarantine tables, read from their files: the replay catalog is not in the
    REST catalog."""
    read = duckdb_rows(stack, [BRONZE, QUARANTINE])
    return Table(sorted(read[BRONZE], key=offset)), Table(sorted(read[QUARANTINE], key=offset))


def replay(stack: Stack, first: int, last: int, *, land_staged: bool = False) -> str:
    app = landing_staged(STAGING_KEY, REPLAY_APP) if land_staged else (REPLAY_APP,)
    output = submit(
        stack, *app, "--topic", TOPIC, "--from-offset", str(first), "--to-offset", str(last)
    )
    return next(line for line in output.splitlines() if line.startswith(f"replay {TOPIC}:"))


def missing_batch() -> list[Json]:
    """Two changes, a failure, and a re-sent copy of the first change at a later offset."""
    resent = envelope("u", after=source_row(ENTITY), lsn=FIRST_LSN + 3)
    return [
        change(ENTITY, 3, "u"),
        change(ENTITY, 4, "u"),
        record(ENTITY, 5, "y"),
        record(ENTITY, 6, resent),
    ]


def commit_sequences(table: Table) -> list[int]:
    return sorted(int(str(row["_commit_sequence"])) for row in table.rows)


def run_replay_scenario(stack: Stack) -> ReplayRuns:
    """Stream two changes and a failure, replay them, then replay a range the stream never saw."""
    streamed_batch = [change(ENTITY, 0, "r"), change(ENTITY, 1, "u"), record(ENTITY, 2, "x")]
    stage(stack, "batch-1", streamed_batch)
    submit(stack, *landing_staged(STAGING_KEY, BRONZE_APP), "--available-now")
    streamed = tables(stack)

    summaries = [replay(stack, 0, 2)]
    replayed_ingested = tables(stack)

    stage(stack, "batch-2", missing_batch())
    summaries.append(replay(stack, 0, 6, land_staged=True))
    replayed_missing = tables(stack)
    summaries.append(replay(stack, 0, 6))
    replayed_again = tables(stack)
    return ReplayRuns(streamed, replayed_ingested, replayed_missing, replayed_again, summaries)
