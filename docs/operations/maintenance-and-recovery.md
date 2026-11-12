# Maintenance and recovery

These jobs change the data outside the streaming path; the [architecture overview](../architecture/overview.md#operations) shows them together.

## Lake maintenance

Streams commit small files and one snapshot per micro-batch, so the bronze and silver Iceberg tables need regular maintenance. `pipelines/maintenance/job.py` runs three Iceberg procedures on every table of the namespaces in `MAINTENANCE_NAMESPACES` (default `bronze,silver`; a namespace that does not exist yet is skipped):

1. `rewrite_data_files` (bin-pack) merges small data files into fewer, larger ones. It commits a `replace` snapshot with the same rows, which Iceberg's streaming reader skips, so the streams reading the table see no new data.
2. `expire_snapshots` removes the snapshots older than `MAINTENANCE_SNAPSHOT_RETENTION_HOURS` (default 7 days), always keeping the newest `MAINTENANCE_RETAIN_LAST` (default 10), together with the files only they referenced. It runs only when no stream reading the table still needs those snapshots (see below).
3. `remove_orphan_files` deletes files no snapshot references, such as the leftovers of a failed write, once they are older than `MAINTENANCE_ORPHAN_AGE_HOURS` (default 3 days). A write still in progress is far younger than that, so its files are never taken; Iceberg refuses anything under 24 hours. It lists the table's files through the table's own FileIO (`prefix_listing`), because the warehouse uses `s3://` paths that Hadoop cannot list.

After the tables, every run calls `refresh_views` (see the silver views), then `refresh_gold_views` (see [gold views in the lake](../lakehouse/gold/writer.md#gold-in-the-lake)), so a changed view definition reaches the lake without a restart; each reports `silver views:` or `gold views:` followed by `created or replaced <n>` or `up to date`. While the silver tables do not exist yet, both report `skipped: ...` instead, which is not a failure.

No step rewrites or deletes rows: the tables stay append-only for the streams that read them. One table, or the view refresh, failing does not stop the others; the job reports every step and fails at the end if any step failed.

```sh
make lake-maintenance                        # run it once, Spark UI on localhost:4044
make up PROFILES=maintenance                 # or keep it running on a schedule
docker compose logs -f maintenance-job       # one summary line per table and run
```

With the `metrics` profile, the **Iceberg tables** dashboard shows each run's effect: the data files and small files of every table drop when it compacts them ([Iceberg tables](monitoring.md#iceberg-tables)).

**Schedule.** The `maintenance-job` service, in its own `maintenance` profile, loops forever: it runs the job with a fresh `spark-submit` (`spark.cores.max=2`, Spark UI on `localhost:4044`), then sleeps `MAINTENANCE_INTERVAL_HOURS` (default 24). Each run is a new application, so the cluster's cores are free between runs, and a failed run is logged and retried at the next interval instead of stopping the loop. `make lake-maintenance` stays available for one-off runs.

**Streams that fall behind.** A stream resumes from the snapshot of its last committed batch, so that snapshot must still exist when it restarts. If a stream (silver reading bronze, gold reading silver) is stopped, or lags, for longer than the snapshot retention, expiring its snapshot would make it fail with `Cannot load current offset at snapshot …` until its checkpoint is reset. `retain_last` protects very little here: with one snapshot per micro-batch, ten snapshots are only minutes of history. So before expiring a table's snapshots, the job reads the checkpoint of every stream that reads the table (`MAINTENANCE_DOWNSTREAM_CHECKPOINTS`; by default each silver stream, `<CHECKPOINT_ROOT>/silver/<entity>`, for its `bronze.<entity>` table, and each gold stream, `<CHECKPOINT_ROOT>/gold/<gold table>`, for the `silver.<entity>` table it reads):

- it takes the Iceberg snapshot in `offsets/<batch>` for the highest batch that also has `commits/<batch>`;
- if that snapshot would be expired (older than the cutoff and not among the newest `MAINTENANCE_RETAIN_LAST`) or is already gone, it skips expiry for that table with a warning and still compacts and removes orphans; the stream catches up from its snapshot, and a later run expires the old snapshots;
- a checkpoint with no committed batch (the stream never started) sets no constraint, and the job says so.

When a stream is reset deliberately, the guard follows its new checkpoint. Iceberg also stores a stream's first snapshot in its checkpoint (`sources/0/offsets/0`), and a restart with `trigger(availableNow=True)` (the jobs' `--available-now` flag) goes back to it: once that snapshot is expired, an `--available-now` run of a stream reading an Iceberg table fails, while the long-running jobs, which use a processing-time trigger, resume normally. Reset the checkpoint of such a one-off run, or run the stream as a long-running job.

## Gold retention

PostgreSQL keeps `GOLD_RETENTION_MONTHS` (default 36, three years) of gold history; the lake keeps all of it. The history facts are partitioned by month on `_changed_at`, so retention drops whole partitions. `pipelines/gold/retention.py` runs `gold_internal.apply_history_retention(keep_months, months_ahead, as_of)` in one transaction, after taking the gold writer lock, so it never runs in the middle of a gold job merge:

1. it gives every month the default partition holds rows of a partition of its own (rows the stream or a reload wrote outside the partitions created so far), and creates the partitions of the current month and the `GOLD_PARTITION_MONTHS_AHEAD` (default 2) months after it;
2. it drops every monthly partition that ends before the first day of the month `GOLD_RETENTION_MONTHS` months before the current one. A month that is only partly outside the window is kept whole, so at least the configured number of full months stays.

The current-state tables and the dimensions are never trimmed, so an order that is still open keeps its current row however old its last change is.

```sh
make gold-retention                         # run it once; prints the partitions it dropped
make up PROFILES=maintenance                # or keep it running on a schedule
docker compose logs -f gold-retention-job   # one summary per run
```

**When it runs.** `make up` and `make pipeline` run it once, as soon as `analytics-db` is healthy, so the monthly partitions exist before the gold job writes; the DDL creates none. Until it has run, the default partition takes every history row, so the gold job never fails for a missing partition; the next run moves those rows into their months. The `gold-retention-job` service, in the `maintenance` profile, keeps it current: it loops forever, running the retention job (plain Python with psycopg, no Spark), then sleeps `GOLD_RETENTION_INTERVAL_HOURS` (default 24). A failed run is logged and retried at the next interval instead of stopping the loop.

## Rebuilding gold from silver

`make gold-rebuild CONFIRM=1` runs `make gold-reset CONFIRM=1` (stops the gold job, recreates the gold schemas, deletes the gold checkpoints), then submits `pipelines/gold/rebuild.py` (Spark UI on `localhost:4045`). The job reads each silver table in batch, keeps the changes whose `_changed_at` is at or before `TO_TIME` when it is set, and writes them through the gold job's own sinks: one staging load and one merge transaction per gold load, the dimensions first, each under the gold writer lock. It finishes with the [retention](#gold-retention) step, so the history window is the one the scheduled retention keeps.

- **Same result as streaming:** the merges are the gold job's, so the rebuilt gold equals the gold the stream builds from the same changes, whatever order they arrived in; only the generated surrogate keys (`customer_key`, `product_key`) get new values.
- **Cutoff:** `TO_TIME` is a source commit time (`_changed_at`), ISO 8601, inclusive, UTC unless it carries an offset. A change committed before it but delivered late is included, so the rebuild shows gold as if every change committed up to that time had been processed.
- **Afterwards** the gold job stays stopped. `make pipeline PIPELINE=gold` starts it on fresh checkpoints: it reads silver from the start, finds every change up to the cutoff already stored (the merges skip them) and catches up with the rest.

## Replaying a landing range into bronze

`analytics/pipelines/src/pipelines/bronze/replay.py` is a one-shot batch job that re-feeds a range of one landing topic into bronze, for example after a fix to a record that was quarantined, or to fill a gap. It reads the topic's landing files in batch (only the files the landing sink's commit log records), keeps the records in the range, parses them with the same `parse_changes` as the bronze stream, and appends only what bronze does not hold yet:

```sh
make bronze-replay TOPIC=finance_db.operations.orders FROM_OFFSET=120 TO_OFFSET=180               # offsets, both ends included
make bronze-replay TOPIC=finance_db.operations.orders FROM_OFFSET=0 TO_OFFSET=50 PARTITION=0      # one Kafka partition only
make bronze-replay TOPIC=finance_db.operations.orders FROM_TIME=2026-11-08T14:00:00 TO_TIME=2026-11-08T15:00:00   # Kafka publish time, UTC unless the time has an offset
```

It prints one summary line: records in range, changes appended, changes already in bronze, records quarantined and records already quarantined. It runs in its own driver container like `make spark-submit` (Spark UI on port 4043, `REPLAY_UI_PORT` to change it, capped at two cores), and reads the same settings as the bronze job.

- **Idempotent by change identity:** a bronze row is appended only when its entity key, `_commit_sequence` and `_change_op` are not already in `lake.bronze.<entity>`, and once even if the range holds the same change twice (a Debezium re-send). Replaying a range that bronze already holds appends nothing.
- **Quarantine without duplicates:** a record that fails validation is appended to `<entity>_quarantine` unless the same source record (`_record_source`, `_source_partition`, `_source_offset`) is already there.
- **Append-only:** it only appends (`writeTo(...).append()`), so the silver stream reading bronze keeps running and sees the replayed changes as new data: its deduplication drops a change it has already versioned, and a replayed change older than what silver holds lands as a late version.
- **Safe next to the running stream** for ranges the stream has already passed. A range the stream has not reached yet may be appended twice, once by each, because the stream does not check what bronze holds; replay those ranges with the stream stopped, or wait until it has passed them.
- **Missing tables are created** like the stream creates them, so a replay into an empty catalog works.

## Rebuilding bronze from landing

When bronze's parsing changes (a new column, a fixed conversion), bronze can be rebuilt from the landing zone, which keeps every record as published:

1. Stop the bronze job and everything that streams from bronze (silver, and gold after it).
2. Keep the old tables aside instead of dropping them: in `make spark-sql`, `ALTER TABLE lake.bronze.<entity> RENAME TO lake.bronze.<entity>_before_rebuild` for each entity and its `_quarantine` table.
3. Delete the bronze checkpoints under `<CHECKPOINT_ROOT>/bronze/` so the job starts again from the first landed file.
4. Run the bronze job with `--available-now` until it has caught up; it creates fresh tables and processes the whole landing zone. Then start it again as a stream.
5. Reset silver as in [Silver layer](../lakehouse/silver.md) (drop its tables and delete `<CHECKPOINT_ROOT>/silver/`), so it versions the new bronze tables from their first snapshot instead of appending every change again, and restart it. Gold, which reads silver, needs the same reset after it.
6. Once the new tables are checked, drop the old ones (`DROP TABLE lake.bronze.<entity>_before_rebuild PURGE`).
