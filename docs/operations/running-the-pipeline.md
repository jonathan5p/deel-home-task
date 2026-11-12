# Running the pipeline

`make pipeline` runs the streaming jobs as long-running Compose services (profile `pipeline`), one driver container per job. Name the job with `PIPELINE`, or use `all` for every job:

To run the whole pipeline as it is meant to run, with its metrics, the REST catalog and the scheduled lake maintenance:

```sh
make up PROFILES="catalog maintenance"            # the stack, Prometheus, the REST catalog, and maintenance every 6 hours
make pipeline PIPELINE=all PROFILES=catalog       # the four streaming jobs and the lake views
```

The `maintenance` profile starts `maintenance-job`, which compacts the bronze and silver tables and expires their old snapshots every `MAINTENANCE_INTERVAL_HOURS` (6 in `.env`), and `gold-retention-job` ([Maintenance and recovery](maintenance-and-recovery.md#lake-maintenance)). Without it the streams still run, but every table keeps every small file and snapshot it ever committed.

```sh
make pipeline PIPELINE=landing        # start the stack, the History Server and the landing job
make pipeline PIPELINE=gold           # the same with the gold job
make pipeline PIPELINE=all            # the same with every job
make pipeline-logs PIPELINE=landing   # follow the job's logs (Ctrl+C stops following, not the job)
make pipeline-stop PIPELINE=landing   # stop and remove the job; the rest of the stack keeps running
make lake-views                       # create or update the silver and gold views once (silver and all also start it)
```

`make pipeline` rebuilds changed images like `make up`, starts the default stack and the Spark History Server, then starts the selected jobs. Running it again after a code change restarts only the jobs whose image changed. A change to the Spark image restarts the cluster, and a job's driver does not survive that; `make pipeline` restarts the jobs along with it, but after a plain `make up`, run `make pipeline` again. Without a valid `PIPELINE` (`landing`, `bronze`, `silver`, `gold` or `all`), the targets fail and list the valid names. `make down` stops the jobs with the rest of the stack.

## Pausing the source changes

To stop new source changes without stopping anything else, pause the Debezium connector:

```sh
make cdc-pause    # the connector stops reading the source; Kafka, the jobs and the stack keep running
make cdc-status   # PAUSED or RUNNING, for the connector and its task
make cdc-resume   # carries on from where it paused
```

While the connector is paused the streaming jobs keep running and simply find no new records, so the lake and gold stay as of the pause. Debezium keeps its position in the source's replication slot, so resuming loses no change; meanwhile PostgreSQL keeps the write-ahead log the slot still needs, so a long pause on a busy source grows its disk use.

| Job | Service | Application UI |
| --- | --- | --- |
| landing | `landing-job` | <http://localhost:4040> |
| bronze | `bronze-job` | <http://localhost:4041> |
| silver | `silver-job` | <http://localhost:4042> |
| gold | `gold-job` | <http://localhost:4045> |

Where to look while a job runs:

- **Application UI** on the job's port (table above), including the Structured Streaming tab with one query per topic.
- **History Server** at <http://localhost:18080>, for current and past runs, since every driver writes event logs to the `spark-events` volume.
- **Landing files** in the SeaweedFS filer UI at <http://localhost:8888/buckets/lake/landing/>, one folder per topic.
- **Spark dashboards** in Grafana, with the `metrics` profile that `make up` starts by default: rates, batch durations and state of every query, and the cluster's cores and memory over time ([Monitoring Spark](monitoring.md)).

Each job service submits its application with the same settings as `make spark-submit` (1-core, 1 GiB executors: `spark.executor.cores=1`, `spark.executor.memory=1g`), plus `spark.cores.max=2` (1 for bronze) and the service name as the driver host, so executors on the workers can connect back to it. The services restart unless they were stopped (`restart: unless-stopped`): a job whose driver fails is restarted and resumes from its checkpoints, without skipping or duplicating records. A container stopped with `make pipeline-stop` or `docker stop`/`docker kill` stays stopped until the next `make pipeline`.

The cluster has two workers with 5 cores and 5 GiB each. Every executor has 1 core and 1 GiB, so a job capped at 2 cores runs two executors, one on each worker while both have a free core. Landing, silver and gold take 2 cores each and bronze 1, so the four streaming jobs use 7 cores and 7 GiB of executor memory, which leaves 3 cores and 3 GiB for ad-hoc runs (`make spark-submit`, `make spark-sql`, `make data-quality`, a scheduled maintenance run), each capped at 2 cores: one gets its 2 cores, a second one starts on the core that is left, and further runs wait for free cores until one ends ([sizing](../architecture/sizing.md#cluster-allocation)).
