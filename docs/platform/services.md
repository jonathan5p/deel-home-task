# Services and storage

Every service runs in one Docker Compose project. The [architecture overview](../architecture/overview.md#components) shows how they connect, and the [Spark cluster](spark.md) page covers Spark.

## Starting the stack

Install [Docker](https://www.docker.com/products/docker-desktop/) with [Compose v2](https://docs.docker.com/compose/install/), GNU Make and [uv](https://docs.astral.sh/uv/) (`make up` uses it to render the gold DDL). With the Docker engine running, run these commands from the repository root:

```sh
make up             # Render the gold DDL, build changed local images and start the default stack (source/CDC, analytics database, analytics API, data lake, Spark cluster)
make up PROFILES="monitoring history"  # Also start optional services (any combination of profiles)
make down           # Stop/remove all project containers, including optional profiles; keep volumes
make spark-submit   # Submit a job to the Spark cluster (defaults to a one-minute UI check job)
make help           # Show lifecycle commands
```

Optional services are grouped in Compose profiles and are not started by a plain `make up`: `monitoring` (Conduktor, see [Optional Kafka monitoring](../operations/debugging.md#optional-kafka-monitoring)), `history` (Spark History Server, see [Spark History Server](spark.md#spark-history-server-optional)), `maintenance` (scheduled lake maintenance and gold retention, see [Lake maintenance](../operations/maintenance-and-recovery.md#lake-maintenance) and [Gold retention](../operations/maintenance-and-recovery.md#gold-retention)) `data-quality` (scheduled data quality checks, see [Data quality checks](../operations/data-quality.md)) and `catalog` (the read-only Iceberg REST catalog, see [Query the lake from DuckDB](spark.md#query-the-lake-from-duckdb)). Pass them with `PROFILES`, separated by spaces.

## Analytics database

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

### Read-only service logins

Consumers of gold never use the `analytics` owner. Each one logs in with a service login of its own, a member of the `gold_reader` role, which can read every view in `gold` and nothing else: no access to `gold_internal`, no writes and no new tables. Sessions start read-only and with a statement timeout.

| Login | Used by | Credentials in `.env` | Statement timeout |
| --- | --- | --- | --- |
| `api_reader` | the analytics API | `API_READER_USER`, `API_READER_PASSWORD` | 5 s |
| `grafana` | the Grafana Gold datasource (see [Grafana](../serving/grafana.md)) | `GRAFANA_READER_USER`, `GRAFANA_READER_PASSWORD` | 30 s |

```sh
docker compose exec -e PGPASSWORD=api_reader analytics-db psql -h localhost -U api_reader -d analytics -c "select * from gold.data_freshness"
```

The lake's [REST catalog](spark.md#query-the-lake-from-duckdb) has a read-only login of its own, `lake_catalog_reader` (`LAKE_CATALOG_READER_USER`, `LAKE_CATALOG_READER_PASSWORD`), which is not a member of `gold_reader`: it can read the tables of the `iceberg_catalog` database and nothing else, and its sessions start read-only.

The gold DDL creates `gold_reader` and its grants, so `make gold-reset` restores them. `docker/analytics-db/initdb/03-service-logins.sh` creates the logins on an empty volume; a new gold login is one more `create_login <login> <password> <statement timeout>` line in it. On an existing volume, create the logins, or update them after changing their credentials in `.env`, with:

```sh
make up          # Recreates analytics-db when its credentials changed
make db-logins   # Creates or updates every service login; safe to repeat
```

A volume created before `gold_reader` existed needs the gold DDL applied first (`make gold-reset CONFIRM=1`, or the `psql` command in [Gold layer](../layers/gold.md#gold-in-postgresql)).

### Schemas and init script

`docker/analytics-db/initdb/01-gold-and-catalog.sql` creates:

- the `gold` schema in the `analytics` database, for the dimensional model;
- the `iceberg_catalog` database with the Iceberg JDBC catalog's own tables, `iceberg_tables` and `iceberg_namespace_properties` (schema version V1), so the read-only REST catalog can start before Spark has written anything. On an older volume Spark created them on first use.

PostgreSQL runs init scripts **only when the data volume is empty**, so editing the script does not change an existing database. To apply a changed script, recreate only the analytics volume (for gold DDL changes, use `make gold-reset` below instead). **Data loss:** this deletes all gold data and the Iceberg catalog, which then no longer points to the lake tables.

```sh
docker compose down -v analytics-db  # Removes only the analytics container and its volume
make up
```

## Data lake (SeaweedFS)

[SeaweedFS](https://github.com/seaweedfs/seaweedfs) provides the S3-compatible object store for the data lake. It runs as a single `seaweedfs` container (`weed mini`), starts with `make up`, and keeps its data in the `seaweedfs-data` volume. It creates the `lake` bucket on startup if it does not exist, and the service reports healthy only once the bucket is there.

| Endpoint | From the host | From other containers |
| --- | --- | --- |
| S3 API | `http://localhost:8333` | `http://seaweedfs:8333` |
| Filer UI (browse buckets and files) | <http://localhost:8888/buckets/lake/> | `http://seaweedfs:8888` |

| Setting | Value |
| --- | --- |
| Access key | `lakeadmin` |
| Secret key | `lakeadmin-secret` |
| Region | any (for example `us-east-1`); use path-style access |

The credentials are local development defaults in `.env` (`LAKE_S3_ACCESS_KEY`, `LAKE_S3_SECRET_KEY`); variables exported in the shell override them. Any S3-compatible store, or AWS S3 itself, can replace SeaweedFS by changing the endpoint and credentials.
