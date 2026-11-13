# Design decisions

The decisions that shaped the project. Each one reads as **Context**, **Decision**, **Why**, **Alternatives considered** and **Outcome**.

The numbers come from my decision log, which also holds smaller decisions, so they have gaps. The linked issues and pull requests hold the discussion.

## Summary

| Decision | Why, in one line |
| --- | --- |
| **Requirements and constraints** | |
| [D8 · Fully local](#d8--fully-local-no-cloud-services) | The assignment asks for a stack anyone can run from the README, and it is the fastest way to develop |
| [D29 · Source side as provided](#d29--the-source-database-and-the-debezium-connector-stay-as-provided) | The assignment treats the source database and the connector as given input |
| **Platform** | |
| [D13 · SeaweedFS instead of MinIO](#d13--object-store-seaweedfs-instead-of-minio) | MinIO is no longer maintained; SeaweedFS is maintained, Apache-2.0 and one container |
| [D9 · Iceberg with a JDBC catalog](#d9--iceberg-with-a-jdbc-catalog-on-the-analytics-postgresql) | The target table format, with no extra catalog service |
| [D14 · Spark 4.1.4 with Iceberg 1.12.0](#d14--spark-414-with-iceberg-1120) | The newest Spark that Iceberg ships a runtime for, in one shared image |
| [D27 · One driver container per application](#d27--one-driver-container-per-application-executors-on-shared-workers) | A driver failure, its memory, logs and UI belong to one application only |
| [D23 · Event logs always written](#d23--event-logs-always-written-optional-history-server) | Finished or crashed applications can still be inspected |
| [D43 · Sizing from measured metrics](#d43--cluster-sizing-and-triggers-from-measured-metrics) | Each setting changed one at a time and measured under the real load |
| **Lake layers** | |
| [D17 · Landing, bronze, silver; gold from silver](#d17--layered-lake-landing-bronze-silver-gold-built-from-silver) | Each layer has one job, and an append-only silver keeps gold streamable |
| [D18 · Landing as raw Avro](#d18--landing-as-raw-avro-files-one-folder-per-topic) | An exact, replayable copy of Kafka |
| [D30 · Readable change identity](#d30--change-identity-in-readable-columns-data-vault-hashing-only-in-part) | Readable keys, with a content hash where it pays off |
| [D34 · Metadata columns start with `_`](#d34--metadata-columns-start-with-_) | Metadata and source data are told apart at a glance |
| [D31 · Late arrivals are history](#d31--late-arrivals-are-history-deduplication-happens-in-silver) | A late change is kept in its place and never overwrites newer state |
| [D36 · Silver deduplication](#d36--silver-deduplication-exact-duplicates-on-write-unchanged-versions-on-read) | Exact duplicates are dropped on write, unchanged versions folded on read |
| **Gold** | |
| [D3 · Gold in PostgreSQL](#d3--gold-in-postgresql-not-clickhouse) | Gold is small and must be consistent after every micro-batch; amended by D44: PostgreSQL serves a copy |
| [D20 · A separate analytics PostgreSQL](#d20--a-separate-analytics-postgresql) | Analytics load never touches the source database |
| [D32 · The gold dimensional model](#d32--the-gold-dimensional-model) | SCD2 dimensions, history facts and current state answer every question |
| [D35 · Public views over an internal schema](#d35--public-views-over-an-internal-schema) | The consumer contract is kept apart from the pipeline's storage |
| [D40 · Indexes from query plans](#d40--gold-indexes-chosen-from-the-query-plans-of-the-current-use-cases) | Only the indexes the business questions use, chosen from their plans |
| [D37 · Retention by monthly partitions](#d37--gold-retention-by-monthly-partitions) | Whole months are dropped instead of deleting rows |
| [D38 · Rebuild gold from silver](#d38--rebuild-gold-from-silver-with-an-optional-cutoff) | Gold can be recreated, up to any point in time, with the streaming code |
| [D39 · Gold in the lake as views](#d39--gold-in-the-lake-as-iceberg-views-over-silver) | Superseded by D44 |
| [D44 · Gold in Spark on Iceberg](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy) | Gold computed once by Spark, in four independent streams, and served by both stores through the same views |
| **Observability** | |
| [D33 · Observability catalog](#d33--pipeline-metrics-in-a-separate-observability-catalog-proposed) | Proposed: per-batch pipeline metrics apart from business data |
| [D42 · Spark metrics in Prometheus](#d42--spark-metrics-sparks-prometheus-servlet-scraped-by-dns-service-discovery) | Spark's own servlet, pulled by Prometheus, with drivers found by their service names |

## Requirements and constraints

### D8 · Fully local, no cloud services

**Context.** The assignment asks for a Dockerized application with a README detailed enough for anyone to run it, and supplies the source database as a Docker Compose stack.

**Decision.** Everything runs in Docker Compose on one machine: the source, Kafka, the Spark cluster, the lake, the analytics database and the serving services.

**Why.**

- It is what the assignment asks for: the whole stack runs with `make up`, without cloud accounts or costs.
- It is also the easiest way to develop: every change is tried in seconds on the same stack, and the infra tests start isolated copies of it, several at a time.

**Alternatives considered.** Managed services (Amazon MSK, EMR, S3, RDS). Closer to production, but running it would need accounts and credentials, and every test run would cost money.

**Outcome.** The local choices keep production in reach: the object store speaks S3 and the catalog is standard Iceberg, so moving to AWS S3 or another catalog is a configuration change. What is left to reach a real production environment is listed in [the way to production](production.md).

### D29 · The source database and the Debezium connector stay as provided

**Context.** The assignment supplies the source database and its Debezium connector, and asks to focus on the analytical pipeline. The connector sends `unity_price` as Debezium's variable-scale decimal, timestamps as epoch milliseconds and dates as days since the epoch.

**Decision.** The pipeline handles the format the supplied connector produces. It changes neither the source DDL nor the connector.

- `unity_price` is decoded at bronze (#34).
- Timestamps and dates are converted at bronze too.

**Why.** The assignment treats the source side as given input; changing it would also change what anyone else's copy of the source produces.

**Alternatives considered.** Setting `decimal.handling.mode=string` on the connector, which sends the decimal as a string, but needs a reset of the topics and the connector.

**Outcome.** Two recommendations for the source owners, not applied here:

- `decimal.handling.mode=string`;
- a publication limited to the four replicated tables (#84).

## Platform

### D13 · Object store: SeaweedFS instead of MinIO

**Context.** The lake needs an S3-compatible object store that runs in Docker Compose and works with Spark's S3A file system and Iceberg. The plan was MinIO, the usual local stand-in for S3. By the time the platform was set up (#10), MinIO was no longer maintained: its GitHub repository was archived, its last release was in October 2025, and its official images could no longer be pulled.

**Decision.** SeaweedFS, as a single `weed mini` container that serves the S3 API and a filer UI, and creates the `lake` bucket on startup (#10).

**Why.**

- It is actively maintained and Apache-2.0 licensed.
- One all-in-one container, with no init step: the bucket is created on startup, and the health check waits for it.
- It speaks the S3 API, so Spark and Iceberg use their standard S3 clients.

**Alternatives considered.** Maintained, open-source, S3-compatible options:

| Option | What it offers | Why not |
| --- | --- | --- |
| `pgsty/minio`, a community fork of MinIO | A drop-in replacement for MinIO | A single, small maintainer |
| RustFS | A newer S3-compatible store | Younger, and less proven with Iceberg |
| Garage | A lightweight S3-compatible store | Needs a cluster layout init step, and covers less of the S3 API |

**Outcome.** Spark writes landing files, Iceberg tables, checkpoints and quality reports to SeaweedFS through the S3 API, as the infra tests check. Any S3-compatible store, or AWS S3 itself, can replace it by changing the endpoint and the credentials ([data lake](infrastructure.md#data-lake-seaweedfs)).

### D9 · Iceberg with a JDBC catalog on the analytics PostgreSQL

**Context.** The lake tables need a table format with snapshots, schema evolution and streaming reads, and that format needs a catalog.

**Decision.** The lake tables are Apache Iceberg. Their catalog is Iceberg's JDBC catalog, in its own `iceberg_catalog` database on the analytics PostgreSQL.

**Why.** Iceberg is the table format of the target stack, and the JDBC catalog avoids running another service.

**Alternatives considered.**

- A REST catalog: closer to production, but one more service.
- Delta Lake.

**Outcome.** A read-only Iceberg REST catalog was added later on top of the same JDBC catalog, so engines such as DuckDB can read the lake (#183, [Iceberg REST catalog](infrastructure.md#iceberg-rest-catalog)). Spark keeps using JDBC.

### D14 · Spark 4.1.4 with Iceberg 1.12.0

**Context.** Iceberg hooks into Spark internals and ships one runtime per Spark minor version. Its newest runtime targets Spark 4.1, while newer Spark releases exist.

**Decision.** One custom image based on `apache/spark:4.1.4-python3` runs the master, the workers and every driver. The Iceberg, AWS bundle and PostgreSQL JDBC jars are pinned with checksums.

**Why.** It is the newest Spark that Iceberg supports, and a shared image guarantees that the driver and executor versions match.

**Alternatives considered.** A newer Spark, which no Iceberg runtime supports yet.

**Outcome.** Upgrading means changing the image tag and the Iceberg runtime once Iceberg supports a newer Spark. An infra test checks that drivers and executors run the same Spark and Python versions.

### D27 · One driver container per application, executors on shared workers

**Context.** The cluster is a Spark standalone master with two workers, and several applications run at once: four streaming jobs, plus one-off jobs and queries. Standalone cluster mode does not support Python applications.

**Decision.** Every application gets a throwaway driver container in client mode: `make spark-submit`, one-off jobs, and one service per streaming job. Its executors run on the shared workers, and each application is capped with `spark.cores.max` (#13).

**Why.** A driver failure, its memory, its logs and its UI port belong to one application only. Client mode in a per-run container is the closest local equivalent of a driver pod.

**Alternatives considered.**

- One long-running driver container, with every application started through `exec`: shared memory, shared lifecycle.
- Drivers inside the `spark-master` container.

**Outcome.** The local setup isolates drivers but shares executors, so each application needs a core cap ([the sizing strategy](sizing.md)). In production:

- on EMR, a step in YARN cluster mode runs the driver inside the cluster, or EMR Serverless runs the job;
- on Kubernetes, the Spark Operator gives each application its own driver and executor pods, with restart policies.

### D23 · Event logs always written, optional history server

**Context.** The streaming jobs run for a long time, and a finished or crashed application leaves no UI to debug it or to take evidence from.

**Decision.** Every driver writes rolling event logs (128 MiB files) to a shared volume. The Spark History Server runs only under the `history` profile, which `make pipeline` starts.

**Why.** Writing the logs costs little, and the History Server, which costs memory, can be started later and still show every past run.

**Alternatives considered.** No History Server at all, the first plan; or one always running.

**Outcome.** Past and crashed runs can be inspected at any time ([Spark History Server](infrastructure.md#spark-history-server-optional)). In production, the event logs would live on object storage, with a cleaner for old logs.

### D43 · Cluster sizing and triggers from measured metrics

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy) for gold: these runs measured gold when its batches were PostgreSQL merges under one lock. The gold trigger is unchanged; gold's current numbers are in [sizing](sizing.md#the-numbers-that-matter).

**Context.** The cluster's settings were estimates: 10-second triggers everywhere, 2 cores per application in whatever executors the master handed out, and no scheduled lake maintenance. The metrics (#195) showed a nearly idle cluster (cores 1–2 % busy, heaps under a quarter of 1 GiB), gold about 40 seconds behind the source, small files never compacted, and the Iceberg table statistics missing from Prometheus.

**Decision.** Six changes, each in its own commit and measured for 20 minutes against the one before, under the source's own generator only:

1. The table statistics exporter skips a table it cannot read instead of failing for all.
2. Bronze and silver trigger every 2 seconds and gold every 5; landing keeps 10.
3. Lake maintenance runs every 6 hours (`maintenance` profile) and keeps 24 hours of snapshots.
4. Every application runs 1-core, 1 GiB executors.
5. Bronze runs on 1 core.
6. Silver commits its RocksDB state as a changelog.

**Why.**

- The source changes once a minute, so shorter downstream triggers add no batches; they only stop each hop from waiting for the next 10-second boundary. Landing keeps 10 seconds because each of its batches writes the files every later layer reads.
- Compaction every 6 hours bounds the small files; 24 hours of snapshots still allow a day of time travel.
- A fixed executor size spreads every 2-core application over both workers, whatever state they are in.
- Bronze used about 1 % of 2 cores; the second core is worth more in the ad-hoc share.
- The changelog avoids uploading a RocksDB snapshot on every silver batch.

**Alternatives considered.**

- A 2-second trigger for landing: more, smaller batches and files for every later layer.
- A load test or a faster generator, to find the real limits: a real source cannot be sped up either, so the jobs are sized for the load they get.
- A rate limit or smaller executors: not needed, and not measurable at this load.
- Hash write distribution for fewer files: a shuffle every batch; compaction is cheaper.

**Outcome.** ([measurements](sizing.md#measured-on-the-real-load))

- Gold latency fell from about 39 seconds to 12–15 seconds at p50; the hops after landing from 9–10 seconds to 1–2.5.
- One maintenance run took bronze from 983 to 16 data files and silver from 1,814 to 20.
- The executor shape and bronze's single core changed no batch time.
- Silver's state commit fell from 134–146 ms to 86–92 ms at p50, with its state kept.
- The jobs never came near their limits, so the core and memory limits could not be sized at this load.
- The data quality checks that read live checkpoints can race a 2-second trigger: two of four full runs each failed one check that passed when run again.

## Lake layers

### D17 · Layered lake: landing, bronze, silver; gold built from silver

**Context.** The assignment needs both history and current state, with consistent results. Spark cannot stream from an Iceberg table that receives MERGE or deletes.

**Decision.**

- **Landing** keeps the records exactly as Kafka delivered them.
- **Bronze** keeps typed, append-only change tables, with a quarantine table for records that fail validation.
- **Silver** keeps append-only version history per entity.
- **Gold** builds the dimensional model from silver.

**Why.** Each layer has one job and can be replayed from the one before it. Silver is append-only so that gold can stream from it.

**Alternatives considered.**

- Two layers, landing and gold, the first plan: fewer hops, but a list of change events is not a usable history, and gold would mix parsing, deduplication and modelling in one job.
- Gold straight from Kafka: the lowest latency, but no durable replay source beyond Kafka's retention.

**Outcome.** Four streaming hops, each with its own checkpoint and commit latency. A fast path from bronze to gold (#87) is the fallback if latency matters.

### D18 · Landing as raw Avro files, one folder per topic

**Context.** The first copy of the data should be exact, cheap to write and replayable.

**Decision.** Each record keeps the Kafka key and value as raw bytes, plus topic, partition, offset and timestamp. The records are stored in Avro files with no partition folders (#21, #22).

**Why.**

- Avro is row-oriented, compact and self-describing, which suits a write-heavy layer.
- Raw bytes stay valid if the topics later move to Avro with a schema registry.
- The Kafka coordinates make replays and gap checks possible.

**Alternatives considered.**

- JSON lines: readable with `cat`, but larger and without a schema.
- A partitioned Iceberg table: no longer the records as received.

**Outcome.** Landing is the replay source for bronze ([replaying a landing range](../operations/maintenance-and-recovery.md#replaying-a-landing-range-into-bronze)), and the `landing` data quality check compares it offset by offset with Kafka.

### D30 · Change identity in readable columns, Data Vault hashing only in part

**Context.** Bronze and silver need to recognise the same change delivered twice, and versions that change nothing. Debezium re-sends a change after a restart at a new Kafka offset.

**Decision.**

- Bronze keeps the source coordinates under technology-neutral names (`_record_source`, `_source_partition`, `_source_offset`, `_published_at`) and the change metadata (`_change_op` in words, `_commit_sequence` from Debezium's `source.lsn`, `_changed_at`, `_captured_at`).
- A change is identified by entity key + `_commit_sequence` + `_change_op`. The Kafka coordinates are lineage, not identity.
- `_content_hash` is a SHA-256 of the business columns, like Data Vault's hash diff. Silver uses it to recognise versions that change nothing, such as re-snapshots and audit-only updates.

**Why.** Readable columns are easy to query and explain, and the hash diff is the one Data Vault idea that pays off here.

**Alternatives considered.** Full Data Vault hash keys. They deduplicate just as well, but give opaque ids and add normalisation risk, for little gain with single integer keys.

**Outcome.** The same identity drives silver's deduplication, the bronze replay and the data quality checks.

### D34 · Metadata columns start with `_`

**Context.** The lake tables mix source columns with columns the pipeline adds.

**Decision.** Every column the pipeline adds to a lake table starts with `_`. Landing keeps Kafka's own column names, because its contract is the raw Kafka record.

**Why.** Readers can tell metadata from source data at a glance.

**Outcome.** The rule holds across bronze, silver and gold, and the [table reference](../reference/index.md) shows it.

### D31 · Late arrivals are history, deduplication happens in silver

**Context.** Changes can arrive late, twice, or out of order: after a connector restart, a replay or a backlog.

**Decision.**

- Bronze is a faithful log: deletes keep their before image, tombstones are skipped, and nothing is deduplicated.
- Silver drops repeated deliveries of the same change. A late change, older than versions already stored, is appended as history: `_valid_to` is derived in the views ordered by `_commit_sequence`, so it lands in its place.
- Gold's current state applies a version only if it is newer than the stored one, so a late version never overwrites the current state.
- Lake maintenance must not outrun the streams. Compaction is safe because streams skip `replace` snapshots. Snapshot expiry checks every downstream stream's checkpoint first and skips a table whose stream still needs a snapshot (#135).

**Why.** History stays complete and in order, and the current state only moves forward.

**Alternatives considered.** A watermark that drops changes older than a threshold, which would lose history.

**Outcome.** The bronze replay (#85) relies on it: replayed changes reach silver as late events, without a restart. Infra tests check that a late version goes into history without becoming current.

### D36 · Silver deduplication: exact duplicates on write, unchanged versions on read

**Context.** Silver receives the same change more than once, and versions that change nothing, such as re-snapshots and audit-only updates.

**Decision.**

- **On write:** the silver stream runs `dropDuplicates` on the change identity, without a watermark and with RocksDB state. A redelivered or replayed change is appended once, and a late change is never dropped (#82).
- **On read:** the `_history` views fold a version into the previous one when its `_content_hash` and `_is_deleted` both match, then derive the validity range. Gold applies the same rule (#136, #137).

**Why.** Exact duplicates are cheap to drop on write; whether a version changes anything depends on its neighbours, which a late change can alter, so it is decided on read.

**Alternatives considered.**

- Python `transformWithState`: needs pandas, pyarrow and protobuf on the executors, and every state call crosses a socket to the JVM.
- A per-batch lookup of silver's latest versions: re-reads silver every batch and needs its own idempotency guard.

**Outcome.** The state grows by one small entry per change and is never cleaned, and the silver table holds more rows than its history view.

## Gold

### D3 · Gold in PostgreSQL, not ClickHouse

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): Spark computes gold into Iceberg, and PostgreSQL serves a copy of the same tables. The reasons below still hold for serving.

**Context.** Gold answers the four business questions in near real time, and the assignment asks for consistent results.

**Decision.** The dimensional model is served from PostgreSQL.

**Why.**

- Gold is small: current state, bounded history and dimensions.
- PostgreSQL applies each micro-batch in one transaction, while ClickHouse's CDC tables deduplicate only eventually, so open orders could be double-counted for a while.

**Alternatives considered.** ClickHouse, which fits billions of fact rows, high query concurrency and scans over the full history better.

**Outcome.** The business questions read small current-state tables through indexes ([D40](#d40--gold-indexes-chosen-from-the-query-plans-of-the-current-use-cases)). ClickHouse stays a candidate for scale.

### D20 · A separate analytics PostgreSQL

**Context.** The assignment allows gold in a schema of the source database or in a new instance.

**Decision.** Gold and the Iceberg catalog live in their own PostgreSQL instance (`analytics-db`), never in the source database.

**Why.** Analytical load and the pipeline's writes never touch the transactional database, and the source stays as provided ([D29](#d29--the-source-database-and-the-debezium-connector-stay-as-provided)).

**Alternatives considered.** A `gold` schema in `finance_db`.

**Outcome.** In production they would be separate too.

### D32 · The gold dimensional model

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the model is unchanged, but Spark computes it. The history facts store business keys, and the views resolve the dimension version valid at each change when read.

**Context.** The four questions need the current state of orders and items, and the assignment also asks for history.

**Decision.**

- **Dimensions:** `dim_customer` and `dim_product` are SCD2, with one row per version and one current row per key. `dim_date` and `dim_order_status` are seeded.
- **History facts:** append-only, one row per change, partitioned by month.
- **Current state:** tables updated in the same transaction, applying only newer versions. Deletes are soft, so a late event cannot bring a deleted row back.
- **Open order:** any status other than `COMPLETED`, `UNKNOWN` and null included. Pending orders are the open orders, and pending items are the items of open orders, reported as a count and a quantity.
- **Top 3:** both top-3 questions keep ties for third place, so they can return more than three rows.

**Why.** The questions read small current-state tables, while history stays queryable "as of" any time.

**Alternatives considered.**

- Current state as a view or materialized view: it slows down as history grows, refreshes in full, and loses old open orders once retention trims history.
- Pending meaning the literal status `PENDING`: inconsistent with the other questions.

**Outcome.** [The gold model](../lakehouse/gold/model.md) describes every table, rule and business-question view.

### D35 · Public views over an internal schema

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the views filter the version columns the gold job stores, and one set of templates renders them over `gold_internal` in PostgreSQL and `lake.gold_internal` in the lake.

**Context.** The gold writer needs columns consumers should not see, such as `_commit_sequence` and soft-deleted rows.

**Decision.** The pipeline writes tables in `gold_internal`, and consumers read views of the same names in `gold`.

- The views leave out pipeline metadata (`_commit_sequence`) and soft-deleted rows.
- They keep the SCD2 validity columns and the change time and type that "as of" queries need.

**Why.** It keeps the pipeline's storage contract apart from the consumer contract. The writer still needs `_commit_sequence` to stay idempotent and to keep late events from overwriting newer state.

**Alternatives considered.** Consumers reading the tables directly, which ties them to the storage.

**Outcome.** The API and Grafana log in with read-only logins that can read `gold` but not `gold_internal` ([read-only service logins](infrastructure.md#read-only-service-logins)).

### D40 · Gold indexes chosen from the query plans of the current use cases

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the partial indexes below are still in use, and an infra test checks with `EXPLAIN` that each view uses its index. The writer indexes are gone: nothing re-resolves the facts' dimension keys any more.

**Context.** The business questions must stay fast as the order history grows, and every index costs the gold writer time on each micro-batch.

**Decision.** The gold query indexes exist only for the current use cases: the four business-question views and `gold.open_orders`, the view that holds the open-order rule. Each index was chosen by reading that query's EXPLAIN plan (#44, #45, #142).

- **`is_open` is stored** on `gold_internal.current_orders`, from `dim_order_status` with the `UNKNOWN` fallback. It is recomputed on every insert and on every newer-version update. This lets the planner see how few orders are open.
- **Partial indexes** on `current_orders`, both `WHERE is_open AND NOT _is_deleted`. They hold only open orders and make the views that read them index-only scans:
    - `current_orders_open_delivery_idx (delivery_date, status_key)` for the two delivery-date views;
    - `current_orders_open_customer_idx (customer_id)` for the top customers.
- **`current_order_items_live_order_idx`** `(order_id) INCLUDE (product_id, quantity) WHERE NOT _is_deleted`: with `is_open` stored, pending items finds the few open orders first, then looks up their items through this index.
- **Dropped:** four current-state indexes that no plan used: `current_orders_status_delivery_idx`, `current_orders_customer_idx`, `current_order_items_order_idx` and `current_order_items_product_idx`.

**Why.** An index nobody's plan uses only slows the writer down; a plan shows which index a query really uses.

**Alternatives considered.** Indexing every column the questions filter or join on, which kept indexes no plan used.

**Outcome.** EXPLAIN on a scratch `postgres:18.6` with 1M orders (about 3% open), 3M items and 50k customers, after `VACUUM ANALYZE`. Ranges are over three runs, as reported in #142.

| View | Before (no `is_open`) | After |
| --- | --- | --- |
| `open_orders_by_delivery_date_and_status` | Parallel Index Only Scan of all live orders + Hash Join, 31–33 ms | Index Only Scan of the open orders (`current_orders_open_delivery_idx`) + Hash Join, 8–10 ms |
| `top_delivery_dates_by_open_orders` | Parallel Index Only Scan of all live orders + Hash Join, 30–35 ms | Index Only Scan of the open orders (`current_orders_open_delivery_idx`), 3–4 ms |
| `pending_items_by_product` | Parallel Hash Join over Seq Scans of `current_order_items` and `current_orders`, 344–380 ms | Nested Loop: Bitmap Heap Scan of the open orders → Index Only Scan of their items (`current_order_items_live_order_idx`), 37–46 ms |
| `top_customers_by_pending_orders` | Nested Loop over the open statuses, Index Only Scan per status, 16–21 ms | Index Only Scan of the open orders (`current_orders_open_customer_idx`), 12–13 ms |

- With `rank()`, both top-3 views still read their partial index with an index-only scan, and PostgreSQL stops ranking early (`Run Condition: rank() <= 3`).
- The plans keep these indexes with a stale visibility map: 10% of rows updated and no vacuum.

New use cases get their indexes the same way: write the query, read its plan on representative data, add the index the plan uses, and drop the ones no plan uses. On the running stack the views answer in under a millisecond ([sizing](sizing.md#postgresql-query-cost)).

### D37 · Gold retention by monthly partitions

**Context.** History facts grow with every change, while the business questions read only current state.

**Decision.** The history facts keep 36 months (`GOLD_RETENTION_MONTHS`), on a schedule and on demand (#48, #149).

- One PostgreSQL function first gives every month in the default partition a partition of its own.
- It then drops whole monthly partitions older than the window.
- Current state and dimensions are never trimmed, and the lake keeps the full history.

**Why.** Dropping a partition is instant and leaves no dead rows to vacuum.

**Alternatives considered.** Row `DELETE`s, or leaving old rows in the default partition, which breaks the window after a reload.

**Outcome.** Retention runs as an optional scheduled service or with `make gold-retention` ([gold retention](../operations/maintenance-and-recovery.md#gold-retention)). The lake's gold views still cover the full history.

### D38 · Rebuild gold from silver, with an optional cutoff

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the rebuild writes silver through the gold job's own writer into the Iceberg tables, then reloads PostgreSQL from them. The surrogate keys stay the same.

**Context.** Gold needs a way back after a logic or schema change, or a corruption, without replaying Kafka.

**Decision.** `make gold-rebuild CONFIRM=1 [TO_TIME=...]` resets gold and loads every silver table in one batch, through the same merge as the stream, up to a `_changed_at` cutoff. It then applies retention (#49, #150).

- A rebuild up to T equals gold streamed from exactly the changes committed up to T, late arrivals included.
- Restarting the gold job replays silver idempotently and continues past the cutoff.

**Why.** Reusing the streaming merge means a rebuild cannot drift from what the stream would have written.

**Alternatives considered.** Restarting the gold job from an empty checkpoint, which has no cutoff and replays every change one micro-batch at a time.

**Outcome.** Infra tests check that a rebuild equals streaming the same changes, with and without a cutoff. Surrogate keys keep their values after a rebuild. The rebuild is one of two recovery operations; the other is the bronze replay:

| | Bronze replay (#133) | Gold rebuild (#150) |
| --- | --- | --- |
| Purpose | Fill gaps: re-read a landing range and append only the changes bronze lacks | Recreate gold from scratch from silver |
| Destructive | No, append-only | Yes: `gold-reset` drops and recreates the gold schemas, drops the gold Iceberg tables and lake gold views, and deletes the gold checkpoints |
| Input | Landing topic and an offset or time range | All of silver, optionally up to `TO_TIME` |
| Deduplication | Anti-join on (key, `_commit_sequence`, `_change_op`); quarantine once per source coordinates | Starts empty; loads through the gold job's own writer |
| Afterwards | Silver picks the rows up as late events; no restart | The gold job is left stopped; `make pipeline` resumes from fresh checkpoints |
| Side effects | None | None: the surrogate keys keep their values |
| When | A missed or failed range | A logic or schema change in gold, or corrupted gold |

Both reuse the streaming code and are tested to equal streaming output. A full bronze rebuild is a manual procedure ([rebuilding bronze](../operations/maintenance-and-recovery.md#rebuilding-bronze-from-landing)). Replaying a silver range into gold would be a cheap extension, because the gold writer is idempotent.

### D39 · Gold in the lake as Iceberg views over silver

**Status.** Superseded by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): gold is now stored as Iceberg tables, and the `lake.gold` views use the same templates as PostgreSQL. Kept as the record of the earlier design.

**Context.** Lake users, such as Spark SQL or DuckDB, should see the same gold model as PostgreSQL, over the full history.

**Decision.** `lake.gold` holds one Iceberg view per public gold relation. Each view is computed at read time from silver, with the same rules as PostgreSQL gold. The gold job writes PostgreSQL only (#86, #152).

- The lake uses natural keys, because PostgreSQL's surrogate keys cannot be reproduced there.
- The `gold` data quality check compares both sides at the silver snapshots gold committed (#154).

**Why.** Views store nothing and need no second writer, so the two sides cannot drift apart unnoticed.

**Alternatives considered.**

- A second sink with MERGE-maintained Iceberg tables: duplicates the incremental SQL and adds cross-stream conflicts.
- Mirroring PostgreSQL into the lake: a CDC-like flow out of the analytics database.
- Append-only lake change tables: duplicates silver.

**Outcome.** Lake reads recompute over the full history. They can be materialised later if needed ([querying the lake](../serving/lake-access.md)).

### D44 · Gold modelled in Spark on Iceberg; PostgreSQL as serving copy

**Context.** I first modelled gold in PostgreSQL: Spark staged each batch, SQL templates computed the SCD2 versions and the merges, and the lake had gold only as views that recomputed the model from silver ([D39](#d39--gold-in-the-lake-as-iceberg-views-over-silver)). Two problems:

- **The two golds could disagree.** Two separate SQL implementations of the same rules, one written per batch and one recomputed on read.
- **The work ran far from the data.** The data and the Spark cluster were in the lake, but gold's compute ran in PostgreSQL, the store that also serves the dashboards.

**Decision.** The gold job computes the gold tables once with Spark, stores them as Iceberg tables in `lake.gold_internal`, copies the same rows into PostgreSQL's `gold_internal`, and both stores serve them through one set of view templates (#221, #224, #225).

| Part | How |
| --- | --- |
| Streams | Four independent streams, one per silver table, each writing only its own tables, with no lock between them |
| SCD2 dimensions | Window functions per business key store `_is_folded`, `_version_number`, `_valid_from`, `_valid_to` and `_is_current`; the surrogate key is `xxhash64(business key, _commit_sequence)` |
| History facts | Each change with its business keys and its own version columns; no dimension key or attribute stored |
| Current state | Applies only newer changes, keeps deletes soft, stores `is_open` |
| Each batch | Computes its rows once, merges each table with one `mergeInto`, then stages the rows in PostgreSQL and runs one upsert transaction that records the Iceberg snapshot ids |
| Views | One set of templates rendered for both stores: point-in-time fact views join the dimension version valid at each change; the four business-question views count the open orders when read |

The views kept their names and columns, so the API and Grafana did not change. [The gold writer](../lakehouse/gold/writer.md) and [the gold model](../lakehouse/gold/model.md) hold the details.

**Why.**

- **One computation, two consistent copies.** Both stores hold the same rows and answer with the same SQL, and PostgreSQL records the Iceberg snapshot it holds, so the gold check compares them exactly.
- **The work runs where the data is,** and PostgreSQL only serves reads.
- **Independent streams, resolved on read.** The usual lakehouse shape for change data, as Databricks' `AUTO CDC` builds it: no stream reads another's tables, so there is no lock, no re-linking and no recount.
- **Deterministic keys.** The same key in both copies and after a rebuild. I hash the LSN, not `_version_number`, because version numbers change when a late version lands between stored ones.
- **Read-time answers are cheap at this size:** the open orders are few, and partial indexes hold only them.

**How it evolved.**

| Step | Design | Result |
| --- | --- | --- |
| 1 | Gold modelled in PostgreSQL; lake gold as views over silver | Two implementations that could disagree |
| 2 | First Spark version: facts linked to dimension versions at write time, per-batch report tables, one lock across the four streams | 42–67 s behind the source at p50; about 170 Spark jobs per order batch |
| 3 | Final design: no write-time linking, no report tables, no lock | 13–16 s behind the source at p50; 11–23 Spark jobs per batch |

**Alternatives considered.**

- **Keep gold's SQL in PostgreSQL and add an Iceberg copy:** two implementations, and the lake copy would trail.
- **Write-time linking of facts to dimension versions:** couples the streams, needs a lock and re-linking when a late version arrives.
- **Report tables written per batch, or a separate report job on a timer:** couples the streams again, or lags gold, for no gain at this size.
- **A streaming `groupBy`:** cannot retract a count when an order changes or completes.
- **Incremental counts with retractions** (as Flink, Kafka Streams or Materialize keep them): needs the previous version of every row and an exactly-once marker. The scaling path once the open orders grow too large ([the way to production](production.md#gold-serving)).
- **PostgreSQL identities as keys:** not reproducible in the lake or after a rebuild.
- **A columnar serving store:** the next step for scale, not needed at this size.

**Outcome.**

- Each store is consistent per stream batch, not across streams: a fact can arrive before its customer or product version, and the views resolve it once the version arrives ([consistency](../lakehouse/gold/writer.md#consistency-between-the-two-copies)).
- On the dev stack on 2026-11-13, gold batches that carry rows take 1.6 s at the median and about 5 s at p95 for customers and products (11 Spark jobs each), 4.9 s and 8.2 s for orders (23 jobs), and 4.3 s and 8.5 s for order items (20 jobs).
- End-to-end gold latency averaged 12.8 s for customers and products, 15.2 s for order items and 16.8 s for orders (5 probes each; p50 13–16 s, p95 16–20 s).
- The business-question views answer in 0.15–0.33 ms through the partial indexes of [D40](#d40--gold-indexes-chosen-from-the-query-plans-of-the-current-use-cases).
- All 68 data quality checks passed, the 28 gold checks among them (report `20261113T054751Z`).

## Observability

### D33 · Pipeline metrics in a separate observability catalog (proposed)

**Context.** Freshness and per-batch statistics are useful beyond the Spark UI, and should not mix with business data.

**Proposal.** Each streaming query records per-batch stats in an Iceberg table, in its own `observability` catalog in the lake (#127): row counts, durations, the last `_commit_sequence` and the last event time. Freshness is then the latest row per query.

**Status.** Deferred to the ideas milestone. Gold's `data_freshness` view covers freshness meanwhile.

### D42 · Spark metrics: Spark's Prometheus servlet, scraped by DNS service discovery

**Context.** The streaming jobs' rates, batch durations and state, and the cluster's use of cores and memory, were only visible in the Spark UI while a driver ran, so neither alerts nor a history to size the cluster from were possible (#69, #126).

**Decision.** Every Spark process serves its own metrics through Spark's `PrometheusServlet` sink, and a Prometheus (`metrics` profile, which `make up` starts by default) scrapes them every 10 seconds, keeping 30 days in a volume. Prometheus finds the drivers by asking Docker's DNS for their Compose service names; `make spark-submit` drivers answer to `spark-app`. The stage and task details that the servlet lacks come from a small exporter that reads the History Server's REST API; the same exporter measures the lake's Iceberg tables (data files, sizes, small files) with PyIceberg through the read-only REST catalog.

**Why.** The servlet ships with Spark and needs only configuration; pulling costs a driver nothing when no Prometheus runs; DNS discovery needs no access to the Docker socket.

**Alternatives considered.**

- The Prometheus JMX exporter as a Java agent in every JVM: one more jar and port per process, and the same metrics under other names.
- A Pushgateway: drivers would push, and it would keep the last value of a driver that died as if it were alive.
- Prometheus's Docker service discovery: finds every container, but needs the Docker socket mounted, which gives root on the host.
- Per-stage metrics from a `SparkListener` in every driver: a JVM plugin to build and ship, and lost with the driver, while the History Server already has every stage in the event logs.

**Outcome.** Spark puts the query and application names in its metric names, so the scrape configuration rewrites them into labels; Grafana's Spark dashboards and two alert rules read the result ([Monitoring Spark](../operations/monitoring.md)). The network effects of shuffles show only as fetch wait time and remote bytes ([network-proxy limits](../operations/monitoring.md#network-proxy-limits)).
