## Data Engineering Take-Home Task

### Welcome

Welcome to Deel's Data Engineering Take-Home task, as mentioned in the Task specification document, this is the pre-built stack that will help you on your solution development. This repository contains a pre-configured database containing the database represented by the following DER:


![Database Diagram](./diagrams/database-diagram.png)


### Database Configuration

Install [Docker](https://www.docker.com/products/docker-desktop/) with [Compose v2](https://docs.docker.com/compose/install/) and GNU Make. With the Docker engine running, run these commands from the repository root:

```sh
make up             # Start the default stack (source/CDC, analytics database, Spark cluster) in detached mode
make up PROFILES="monitoring history"  # Also start optional services (any combination of profiles)
make down           # Stop/remove all project containers, including optional profiles; keep volumes
make spark-submit   # Submit a job to the Spark cluster (defaults to a one-minute UI check job)
make help           # Show lifecycle commands
```

Optional services are grouped in Compose profiles and are not started by a plain `make up`: `monitoring` (Conduktor, see **Optional Kafka monitoring**) and `history` (Spark History Server, see **Spark History Server**). Pass them with `PROFILES`, separated by spaces.

#### Destructive reset

**Data loss:** `make clean CONFIRM=1` removes this Compose project's containers and volumes, including persisted database, Kafka, and optional monitor data. Use it only when you intend to discard that data. It does not delete images or source files, or perform a global Docker prune. `make clean` refuses before invoking Docker unless GNU Make evaluates `CONFIRM` to exactly `1`. Make normalizes leading assignment whitespace, so `make clean 'CONFIRM= 1'` also explicitly confirms deletion.

All lifecycle targets use the same Compose project selection; `make down` preserves its volumes. Keep the same working directory and Compose environment between commands. `COMPOSE` defaults to `docker compose` and can be overridden for a compatible CLI or a harmless test probe. Compose failures cause Make to fail.

Once you have the Database up and running feel free to connect to this using any tool you want, for this you can use the following credentials:

- **Username**: `finance_db_user`
- **Password**: `1234`
- **Database**: `finance_db`

### Analytics database

A separate PostgreSQL instance (`analytics-db`) holds the gold layer and the Iceberg catalog, so analytical load never reaches the source database. It starts with `make up` and keeps its data in the `analytics-pg-data` volume across `make down` / `make up`.

| Setting | From the host | From other containers |
| --- | --- | --- |
| Host and port | `localhost:5433` | `analytics-db:5432` |
| Username | `analytics` | `analytics` |
| Password | `analytics` | `analytics` |
| Database | `analytics` | `analytics` |

The credentials are local development defaults in `.env` (`ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD`, `ANALYTICS_DB_NAME`); variables exported in the shell override them.

```sh
docker compose exec analytics-db psql -U analytics -d analytics
```

#### Schemas and init script

`docker/analytics-db/initdb/01-gold-and-catalog.sql` creates:

- the `gold` schema in the `analytics` database, for the dimensional model;
- the `iceberg_catalog` database, where the Iceberg JDBC catalog creates its own tables on first use.

PostgreSQL runs init scripts **only when the data volume is empty**, so editing the script does not change an existing database. To apply a changed script, recreate only the analytics volume. **Data loss:** this deletes all gold data and the Iceberg catalog, which then no longer points to the lake tables.

```sh
docker compose down -v analytics-db  # Removes only the analytics container and its volume
make up
```

### Optional Kafka monitoring

With the source stack already running, start only the optional Conduktor services:

```sh
docker compose --profile monitoring up -d --no-deps --wait conduktor-db
docker compose --profile monitoring up -d --no-deps --wait conduktor-console
```

Open http://localhost:8090 and sign in with `admin@local.test` / `LocalConsole123!`. These are public development-only credentials. The current `8090:8080` mapping publishes on all host interfaces; use `127.0.0.1:8090:8080` if localhost-only binding is required. Console connects to the existing broker at `kafka:29092`.

Only Console and its metadata PostgreSQL database are added. Topic management, message browsing/production, and basic cluster inspection do not require Cortex. Historical metrics dashboards and alerting are not included; no ksqlDB service is required.

Stop only monitoring, keeping the source stack and stored data:

```sh
docker compose --profile monitoring stop conduktor-console conduktor-db
```

The Conduktor integration is prepared but runtime validation is still pending. `make down` stops the entire project; use the service-specific command above when leaving the source running.

### Debezium CDC

The stack includes a Debezium CDC pipeline that streams database changes to Kafka in real-time. Kafka is available at `localhost:9092`.

#### Topics

| Kafka Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

#### Kafka Connection Example

```properties
bootstrap.servers=localhost:9092
```

### Spark cluster

`make up` also starts a Spark 4.1.4 standalone cluster: one master and two workers built from `docker/spark/Dockerfile`. The same image is meant to run the driver, so driver and worker versions always match.

| Service | UI | Resources |
|---|---|---|
| `spark-master` | http://localhost:8080 | Cluster manager; accepts jobs at `SPARK_MASTER_URL` |
| `spark-worker-1` | http://localhost:8081 | 2 cores, 2 GiB executor memory |
| `spark-worker-2` | http://localhost:8082 | 2 cores, 2 GiB executor memory |
| `spark-history` (optional) | http://localhost:18080 | History server for finished, crashed and running applications; started with `make spark-history` |
| Running application | http://localhost:4040 | Jobs, stages, SQL and streaming tabs of the driver started by `make spark-submit`; only available while it runs |

Worker resources are set with `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` in `docker-compose.yaml`. They cap what executors can claim on each worker; the master, worker and driver JVMs need memory on top of that, so make sure Docker has enough memory for the whole stack.

`SPARK_MASTER_URL` defaults to `spark://spark-master:7077` in `.env`, which Docker Compose loads automatically. Exporting the variable in your shell overrides it; recreate the Spark services (`make up`) after changing it.

#### Submitting a job

```sh
make spark-submit                                                                                # Run the one-minute UI check job (default)
make spark-submit SPARK_APP=/opt/spark/examples/src/main/python/pi.py SPARK_APP_ARGS=10         # Run another application with arguments
```

`SPARK_APP` is a path inside the `spark-master` container and defaults to `/opt/spark/jobs/spark_ui_check.py`; `SPARK_APP_ARGS` are passed to it unchanged. The driver runs in client mode inside the `spark-master` container and submits to that container's `SPARK_MASTER_URL`, so the cluster must be running first. Running and finished applications appear on the master UI.

Interrupting `make spark-submit` does not stop a driver that is already running inside the container, and that driver keeps holding the workers' cores, so the next submission waits for resources. Stop leftover drivers with `docker compose exec spark-master pkill -f org.apache.spark.deploy.SparkSubmit`.

#### Spark History Server (optional)

Every driver writes event logs to the `spark-events` volume, whether or not the history server is running. Logs roll over at 128 MiB, so long-running streaming applications stay in bounded files. Start the history server only when you need to inspect past runs:

```sh
make spark-history                                   # Start only the history server (http://localhost:18080)
make up PROFILES=history                             # Or start it together with the rest of the stack
docker compose --profile history stop spark-history  # Stop it again; event logs are kept
```

When it starts, it lists every application whose event logs are in the volume, including runs that happened while it was stopped. `make down` stops it with the rest of the stack, and `make clean CONFIRM=1` deletes the event logs.

#### Checking the application UI

The application UI at http://localhost:4040 exists only while a driver runs. The default job, [`jobs/spark_ui_check.py`](jobs/spark_ui_check.py), is a dummy streaming query that runs for one minute so the UI can be inspected. It reads Spark's built-in `rate` source and counts rows per 10-second window every five seconds. The `jobs/` directory is mounted read-only into the master container at `/opt/spark/jobs`. While the job runs, open http://localhost:4040:

| Tab | What to look at |
|---|---|
| Jobs and Stages | One job per micro-batch; the windowed count adds a shuffle stage |
| SQL / DataFrame | The plan of each micro-batch |
| Executors | Executors on both workers |
| Structured Streaming | The `rate_window_counts` query, with input and processing rates and batch durations |

The job prints the progress of the latest micro-batch every five seconds. When it finishes, the UI on port 4040 closes.

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.

For any questions, feel free to reach us out through data-platform@deel.com
