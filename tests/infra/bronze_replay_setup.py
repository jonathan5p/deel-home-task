import json
from dataclasses import dataclass

from tests.infra.bronze_setup import (
    FIRST_LSN,
    LAND_APP,
    ROWS_APP,
    ROWS_PREFIX,
    TOPICS,
    Json,
    Rows,
    change,
    envelope,
    offset,
    record,
    source_row,
)
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
BRONZE = f"{CATALOG}.bronze.{ENTITY}"
QUARANTINE = f"{CATALOG}.bronze.{ENTITY}_quarantine"


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


def land(stack: Stack, batch: str, records: list[Json]) -> None:
    stack.put_object(f"{STAGING_KEY}/{TOPIC}/{batch}.avro", landing_avro(records))
    submit(stack, LAND_APP, f"s3a://lake/{STAGING_KEY}")


def tables(stack: Stack) -> tuple[Table, Table]:
    dumps = {}
    for line in submit(stack, ROWS_APP, BRONZE, QUARANTINE).splitlines():
        if line.startswith(ROWS_PREFIX):
            dump = json.loads(line.removeprefix(ROWS_PREFIX))
            dumps[dump["table"]] = Table(sorted(dump["rows"], key=offset))
    return dumps[BRONZE], dumps[QUARANTINE]


def replay(stack: Stack, first: int, last: int) -> str:
    output = submit(
        stack, REPLAY_APP, "--topic", TOPIC, "--from-offset", str(first), "--to-offset", str(last)
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
    land(stack, "batch-1", [change(ENTITY, 0, "r"), change(ENTITY, 1, "u"), record(ENTITY, 2, "x")])
    submit(stack, BRONZE_APP, "--available-now")
    streamed = tables(stack)

    summaries = [replay(stack, 0, 2)]
    replayed_ingested = tables(stack)

    land(stack, "batch-2", missing_batch())
    summaries.append(replay(stack, 0, 6))
    replayed_missing = tables(stack)
    summaries.append(replay(stack, 0, 6))
    replayed_again = tables(stack)
    return ReplayRuns(streamed, replayed_ingested, replayed_missing, replayed_again, summaries)
