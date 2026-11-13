# Maintenance and recovery

These jobs change the data outside the streaming path; the [architecture overview](../architecture/overview.md#operations) shows them together.

## Lake maintenance

Streams commit small files and a snapshot per batch, and gold's merges add position delete files. The maintenance job runs four Iceberg procedures on every table in `MAINTENANCE_NAMESPACES` (default `bronze,silver,gold_internal`):

| Step | What it does |
| --- | --- |
| `rewrite_data_files` | Merges small files. Commits a `replace` snapshot, which streams skip |
| `rewrite_position_delete_files` | Merges gold's delete files and drops those that no longer apply |
| `expire_snapshots` | Removes snapshots older than `MAINTENANCE_SNAPSHOT_RETENTION_HOURS` (24 in `.env`), keeping the newest `MAINTENANCE_RETAIN_LAST` (10), only when no stream still needs them |
| `remove_orphan_files` | Deletes unreferenced files older than `MAINTENANCE_ORPHAN_AGE_HOURS` (3 days) |

- Both rewrites commit each file group on its own, so a gold merge during compaction fails only the groups it conflicts with, and the gold job replays its own batch.
- Each run then refreshes the silver and lake gold views, reporting `skipped` while the tables do not exist.
- No step changes what a table holds. One failing table does not stop the others; the job fails at the end.

```sh
make lake-maintenance                        # run once, Spark UI on localhost:4044
make up PROFILES="catalog maintenance"       # or on a schedule
docker compose logs -f maintenance-job       # one summary line per table and run
```

The `maintenance-job` service runs the job, then sleeps `MAINTENANCE_INTERVAL_HOURS` (6 in `.env`); a failed run is retried at the next interval. The **Iceberg tables** dashboard shows each run's effect ([Iceberg tables](monitoring.md#iceberg-tables)). Every 6 hours keeps at most a quarter of a day of small files, and 24 hours of snapshots still allow a day of time travel ([D43](../architecture/decisions.md#d43--cluster-sizing-and-triggers-from-measured-metrics)).

**Streams that fall behind.** A stream resumes from the snapshot of its last committed batch, so expiring it would break the stream. Before expiring a table, the job reads the checkpoint of every stream that reads it (`MAINTENANCE_DOWNSTREAM_CHECKPOINTS`: silver for bronze, gold for silver) and skips expiry, with a warning, when that snapshot would go. It still compacts and removes orphans; a later run expires once the stream catches up.

An `--available-now` restart goes back to the stream's first snapshot, so it fails once that snapshot has expired. The long-running jobs are not affected; reset the checkpoint of such a one-off run.

## Gold retention

PostgreSQL keeps `GOLD_RETENTION_MONTHS` (36) of history; the Iceberg tables keep all of it. The retention job runs `gold_internal.apply_history_retention` in one transaction under the exclusive advisory lock (the gold copies take it shared):

1. Move rows from the default partition into monthly partitions, and create the current month and the next `GOLD_PARTITION_MONTHS_AHEAD` (2).
2. Drop every monthly partition that ends before the window. A month partly inside it is kept.

Current state and dimensions are never trimmed, so an open order keeps its row however old it is.

```sh
make gold-retention                         # run once; prints the partitions it dropped
make up PROFILES=maintenance                # or on a schedule
docker compose logs -f gold-retention-job   # one summary per run
```

`make up` and `make pipeline` run it once when `analytics-db` is healthy. Until then the default partition takes every history row. The `gold-retention-job` service (plain Python, no Spark) then runs it every `GOLD_RETENTION_INTERVAL_HOURS` (24).

## Rebuilding gold from silver

`make gold-rebuild CONFIRM=1 [TO_TIME=...]` runs `make gold-reset CONFIRM=1`, then a batch job (Spark UI on `localhost:4045`) that recreates the gold tables and views, writes each silver table up to `TO_TIME` through the gold job's own writer, reloads PostgreSQL from Iceberg and applies [retention](#gold-retention).

- **Same result as streaming,** whatever order the changes arrived in. Surrogate keys keep their values.
- **Cutoff:** `TO_TIME` is a source commit time (`_changed_at`), ISO 8601, inclusive, UTC unless it has an offset. Late-delivered changes committed before it are included.
- **Afterwards** the gold job stays stopped. `make pipeline PIPELINE=gold` starts it on fresh checkpoints; it finds every change up to the cutoff already stored and catches up.

## Replaying a landing range into bronze

A one-shot batch job re-feeds a range of one landing topic into bronze, for example after fixing a quarantined record or to fill a gap. It parses the range like the bronze stream and appends only what bronze does not hold yet.

```sh
make bronze-replay TOPIC=finance_db.operations.orders FROM_OFFSET=120 TO_OFFSET=180               # offsets, both ends included
make bronze-replay TOPIC=finance_db.operations.orders FROM_OFFSET=0 TO_OFFSET=50 PARTITION=0      # one Kafka partition
make bronze-replay TOPIC=finance_db.operations.orders FROM_TIME=2026-11-08T14:00:00 TO_TIME=2026-11-08T15:00:00   # Kafka publish time
```

It prints one summary line and runs on 2 cores, UI on port 4043 (`REPLAY_UI_PORT`).

- **Idempotent:** a change is appended only when its identity is not in bronze yet; a record is quarantined only once.
- **Append-only,** so silver keeps streaming and sees the replayed changes as new or late versions.
- **Safe next to the stream** for ranges the stream has passed. A range it has not reached may be appended twice; replay that with the stream stopped.

## Rebuilding bronze from landing

When bronze's parsing changes, rebuild it from landing:

1. Stop bronze and everything downstream (silver, gold).
2. In `make spark-sql`, rename each bronze and quarantine table to `<table>_before_rebuild`.
3. Delete `<CHECKPOINT_ROOT>/bronze/`.
4. Run the bronze job with `--available-now` until it catches up, then start it as a stream.
5. [Reset silver](../lakehouse/silver.md#resetting-silver), then gold, and restart them.
6. Once the new tables are checked, drop the old ones (`DROP TABLE ... PURGE`).
