# Running the pipeline

`make pipeline` runs the streaming jobs as long-running Compose services (profile `pipeline`), one driver container per job. Name the job with `PIPELINE`, or use `all` for every job:

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
- **Spark dashboards** in Grafana, with the `metrics` profile (`make up PROFILES=metrics`): rates, batch durations and state of every query, and the cluster's cores and memory over time ([Monitoring Spark](monitoring.md)).

Each job service submits its application with the same settings as `make spark-submit`, plus `spark.cores.max=2` and the service name as the driver host, so executors on the workers can connect back to it. The services restart unless they were stopped (`restart: unless-stopped`): a job whose driver fails is restarted and resumes from its checkpoints, without skipping or duplicating records. A container stopped with `make pipeline-stop` or `docker stop`/`docker kill` stays stopped until the next `make pipeline`.

The cluster has two workers with 5 cores and 5 GiB each, and each job takes at most 2 cores. The four streaming jobs (landing, bronze, silver and gold) use 8 cores and up to 8 GiB of executor memory, which leaves 2 cores and 2 GiB for ad-hoc runs (`make spark-submit`, `make spark-sql`, `make data-quality`); further runs wait for free cores until one ends. Each executor takes Spark's default 1 GiB.
