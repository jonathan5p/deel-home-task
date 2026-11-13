# Running the pipeline

`make pipeline` runs the streaming jobs as long-running Compose services, one driver container per job. To run everything as intended, with metrics, the REST catalog and scheduled maintenance:

```sh
make up PROFILES="catalog maintenance"         # the stack, Prometheus, the REST catalog and maintenance every 6 hours
make pipeline PIPELINE=all PROFILES=catalog    # the four streaming jobs and the lake views
```

Without the `maintenance` profile the streams still run, but the tables keep every small file and snapshot they commit ([lake maintenance](maintenance-and-recovery.md#lake-maintenance)).

```sh
make pipeline PIPELINE=landing        # the stack, the History Server and one job (landing, bronze, silver, gold or all)
make pipeline-logs PIPELINE=landing   # follow the job's logs
make pipeline-stop PIPELINE=landing   # stop the job; the rest keeps running
make lake-views                       # create or update the silver and gold views once
```

- Running `make pipeline` again after a code change restarts only the jobs whose image changed. After a plain `make up` that rebuilt the Spark image, run `make pipeline` again: drivers do not survive a cluster restart.
- The services restart unless stopped: a failed driver resumes from its checkpoints without skipping or duplicating records. A job stopped with `make pipeline-stop` stays stopped.
- Each job runs 1-core, 1 GiB executors, capped at 2 cores (bronze 1), so the streams use 7 of the 10 cores ([sizing](../architecture/sizing.md#cluster-allocation)).

| Job | Service | Application UI |
| --- | --- | --- |
| landing | `landing-job` | <http://localhost:4040> |
| bronze | `bronze-job` | <http://localhost:4041> |
| silver | `silver-job` | <http://localhost:4042> |
| gold | `gold-job` | <http://localhost:4045> |

Where to look while a job runs:

- **Application UI** on the job's port, with the Structured Streaming tab.
- **History Server** at <http://localhost:18080>, for current and past runs.
- **Landing files** at <http://localhost:8888/buckets/lake/landing/>.
- **Spark dashboards** in Grafana ([Monitoring Spark](monitoring.md)).

## Pausing the source changes

```sh
make cdc-pause    # the connector stops reading the source; everything else keeps running
make cdc-status   # PAUSED or RUNNING
make cdc-resume   # carries on from where it paused
```

While paused, the jobs find no new records and gold stays as of the pause. Debezium keeps its position in the replication slot, so no change is lost; PostgreSQL keeps the WAL the slot needs, so a long pause grows its disk use.
