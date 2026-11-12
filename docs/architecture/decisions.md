# Design decisions

The decisions that shaped the project, why they were made, what else was considered, and how they turned out. Each one reads as **Context**, **Decision**, **Why**, **Alternatives considered** and **Outcome**, with evidence where it matters.

The numbers (D3, D13, ...) come from the project's decision log, and other pages link them. The log also holds smaller and superseded decisions, so the numbers here have gaps. The issues and pull requests linked under each decision hold the discussion and the change.

## Summary

| Decision | Why, in one line |
| --- | --- |
| **Requirements and constraints** | |
| [D8 · Fully local](#d8--fully-local-no-cloud-services) | The assignment asks for a stack reviewers can run from the README, and it is the fastest way to develop |
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
| [D44 · Gold in Spark on Iceberg](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy) | One engine for every transformation, columnar gold, deterministic keys, PostgreSQL only for serving |
| **Observability** | |
| [D33 · Observability catalog](#d33--pipeline-metrics-in-a-separate-observability-catalog-proposed) | Proposed: per-batch pipeline metrics apart from business data |
| [D42 · Spark metrics in Prometheus](#d42--spark-metrics-sparks-prometheus-servlet-scraped-by-dns-service-discovery) | Spark's own servlet, pulled by Prometheus, with drivers found by their service names |

## Requirements and constraints

### D8 · Fully local, no cloud services

**Context.** The assignment asks for a Dockerized application with a README detailed enough for reviewers to run it, and supplies the source database as a Docker Compose stack.

**Decision.** Everything runs in Docker Compose on one machine: the source, Kafka, the Spark cluster, the lake, the analytics database and the serving services.

**Why.**

- It is what the assignment asks for: reviewers run the whole stack with `make up`, without cloud accounts or costs.
- It is also the easiest way to develop: every change is tried in seconds on the same stack, and the infra tests start isolated copies of it, several at a time.

**Alternatives considered.** Managed services (Amazon MSK, EMR, S3, RDS). Closer to production, but reviewers would need accounts and credentials, and every test run would cost money.

**Outcome.** The local choices keep production in reach: the object store speaks S3 and the catalog is standard Iceberg, so moving to AWS S3 or another catalog is a configuration change. What is left to reach a real production environment is listed in [the way to production](production.md).

### D29 · The source database and the Debezium connector stay as provided

**Context.** The assignment supplies the source database and its Debezium connector, and asks to focus on the analytical pipeline. The connector sends `unity_price` as Debezium's variable-scale decimal, timestamps as epoch milliseconds and dates as days since the epoch.

**Decision.** The pipeline handles the format the supplied connector produces. It changes neither the source DDL nor the connector.

- `unity_price` is decoded at bronze (#34).
- Timestamps and dates are converted at bronze too.

**Why.** The assignment treats the source side as given input; changing it would also change what the reviewers' own source produces.

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

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy) where it discusses gold: the measurements below were taken when gold's batches were PostgreSQL merges under one advisory lock. Gold's batches are now Spark transforms, Iceberg commits and PostgreSQL upserts; its trigger is unchanged.

**Context.** The cluster's settings were estimates: 10-second triggers everywhere, 2 cores per application in whatever executors the master handed out, and lake maintenance only when someone started its profile. A first look at the metrics (#195) showed the cluster nearly idle (each job's cores busy 1–2 % of the time, executor heaps under a quarter of 1 GiB, no spill), gold about 40 seconds behind the source because every hop waited one full 10-second interval, bronze and silver tables growing by about 60–120 small files an hour that were never compacted, and the Iceberg table statistics missing from Prometheus.

**Decision.** Six changes, each in its own commit and measured on its own for 20 minutes against the one before, under the source's own generator only:

1. The table statistics exporter measures every table even when one has an empty snapshot (the quarantine tables'), which had stopped it from publishing any series.
2. Landing keeps its 10-second trigger; bronze and silver trigger every 2 seconds and gold every 5 (`.env`). The batch-over-trigger alert and the streaming dashboard compare each job with its own trigger, which Prometheus records per job.
3. Lake maintenance runs every 6 hours as part of the pipeline (`maintenance` profile) and keeps 24 hours of snapshots.
4. Every application runs 1-core, 1 GiB executors (`spark.executor.cores=1`, `spark.executor.memory=1g`).
5. Bronze runs on 1 core.
6. Silver commits its RocksDB state as a changelog.

**Why.**

- The source changes in one burst a minute, so shorter downstream triggers add no batches; they only stop each hop from waiting for the next 10-second boundary. Landing keeps 10 seconds because it reads Kafka and writes the files every later layer reads.
- Gold's batches are bound by PostgreSQL merges under one lock, not by Spark, so its trigger only has to be shorter than the wait it removes.
- Compaction every 6 hours bounds the small files a table carries to a quarter of a day; 24 hours of snapshots still allow a day of time travel, while every commit rewrites a far shorter snapshot list than with 7 days. Expiry already skips tables whose readers still need an older snapshot.
- A fixed executor size makes `spark.cores.max` mean a number of executors, spread over both workers, whatever state the workers are in.
- Bronze used about 1 % of 2 cores; its second core is worth more in the ad-hoc share.
- The changelog avoids uploading a RocksDB snapshot on every silver batch, and Spark documents the switch on an existing checkpoint as supported in both directions; silver's checkpoint was backed up first and kept its state.

**Alternatives considered.**

- A 2-second trigger for landing as well: landing's wait in Kafka is the largest hop left, but landing writes one Avro file per topic per batch for every later layer to read, and the generator's burst a minute would then be split into more, smaller batches and files.
- A load test or a faster generator to find the cores and memory the jobs really need: rejected by the user, because a real source cannot be sped up either; the jobs are sized for the load they get.
- A rate limit (`maxOffsetsPerTrigger`) or smaller executors: not needed at this load, and not measurable without more of it.
- Hash write distribution or partitioning changes to make fewer files per commit: compaction is cheaper and keeps the streams' batches free of a shuffle.

**Outcome.** Measured on the real load ([sizing](sizing.md#measured-on-the-real-load)):

- Gold latency fell from about 39 seconds after the source commit (p50, 43–50 at p95) to about 12–15 seconds (products up to 18), with p95 between 16 and 34 seconds, the high end when a change arrived behind a slow dimension batch; the hops after landing went from 9–10 seconds each to 1–2.5.
- One maintenance run rewrote 182–595 small files per table into 2, taking bronze from 983 to 16 data files and silver from 1,814 to 20, and the average data file from about 17 and 11 KiB to 120 and 75 KiB right after it. Bronze and silver batches were also faster just after compaction (bronze p95 1.0 s to 0.6 s), and slowed again as the files came back.
- The executor shape and bronze's single core changed no batch time; every 2-core job ran one executor on each worker, and bronze on one core kept its p95 near 1 second.
- Silver's state commit time fell from 134–146 ms at p50 (181–201 at p95) in the run before to 86–92 ms (126–147 at p95), with no snapshot upload left on the commit path; silver restarted on its existing checkpoint with its state and passed its data quality checks.
- The jobs never came near their limits (cores 0.2–2.3 % busy, executor heaps at most 260 MiB of 1 GiB, GC under 3 %, no spill, no failed tasks), so the core and memory limits could not be sized at this load.
- Gold's dimension batches take 10–19 seconds at p95, longer than the 5-second trigger, and grew over the afternoon; that is PostgreSQL time and is not changed by any of these settings.
- The data quality checks that read live checkpoints can race a 2-second trigger: two of four full runs each failed one bronze or silver check that passed when run again, and the last full run passed all 53.

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

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): Spark computes gold and stores it as Iceberg tables; PostgreSQL keeps serving the business questions, as a copy filled by plain upserts. The reasons below still hold for serving.

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

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the model below is unchanged, but Spark computes it and the history facts store their version columns. The top 3 still uses `rank()`, and the four questions read report tables kept by the gold job.

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

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the views now only select and filter columns the gold job stores (`_is_folded`, `_valid_to`, `_is_current`); they no longer compute the fold or the validity ranges. The lake has the same views over the gold Iceberg tables.

**Context.** The gold writer needs columns consumers should not see, such as `_commit_sequence` and soft-deleted rows.

**Decision.** The pipeline writes tables in `gold_internal`, and consumers read views of the same names in `gold`.

- The views leave out pipeline metadata (`_commit_sequence`) and soft-deleted rows.
- They keep the SCD2 validity columns and the change time and type that "as of" queries need.

**Why.** It keeps the pipeline's storage contract apart from the consumer contract. The writer still needs `_commit_sequence` to stay idempotent and to keep late events from overwriting newer state.

**Alternatives considered.** Consumers reading the tables directly, which ties them to the storage.

**Outcome.** The API and Grafana log in with read-only logins that can read `gold` but not `gold_internal` ([read-only service logins](infrastructure.md#read-only-service-logins)).

### D40 · Gold indexes chosen from the query plans of the current use cases

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the business-question views read report tables the gold job keeps, one row per answer, so the partial indexes below and the writer indexes are no longer needed. The method stays for any new query over the larger tables.

**Context.** The business questions must stay fast as the order history grows, and every index costs the gold writer time on each micro-batch.

**Decision.** The gold query indexes exist only for the current use cases: the four business-question views and `gold.open_orders`, the view that holds the open-order rule. Each index was chosen by reading that query's EXPLAIN plan (#44, #45, #142).

- **`is_open` is stored** on `gold_internal.current_orders`, from `dim_order_status` with the `UNKNOWN` fallback. It is recomputed on every insert and on every newer-version update. This lets the planner see how few orders are open.
- **Partial indexes** on `current_orders`, both `WHERE is_open AND NOT _is_deleted`. They hold only open orders and make the views that read them index-only scans:
    - `current_orders_open_delivery_idx (delivery_date, status_key)` for the two delivery-date views;
    - `current_orders_open_customer_idx (customer_id)` for the top customers.
- **`current_order_items_live_order_idx`** `(order_id) INCLUDE (product_id, quantity) WHERE NOT _is_deleted`: with `is_open` stored, pending items finds the few open orders first, then looks up their items through this index.
- **Dropped:** four current-state indexes that no plan used: `current_orders_status_delivery_idx`, `current_orders_customer_idx`, `current_order_items_order_idx` and `current_order_items_product_idx`.
- **Writer indexes** serve the gold writer, not the queries: the unique current-row indexes on the dimensions, and the fact indexes on `customer_id` and `product_id` used to re-resolve dimension keys.

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

New use cases get their indexes the same way: write the query, read its plan on representative data, add the index the plan uses, and drop the indexes no plan uses any more. [The sizing strategy](sizing.md#postgresql-query-cost) tracks the same queries on the real run.

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

**Status.** Amended by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the rebuild runs the gold job's Spark transforms over all of silver in one batch into the Iceberg tables, then reloads PostgreSQL from them. The surrogate keys are deterministic, so a rebuild keeps them; the rows below that say they get new values describe the previous design.

**Context.** Gold needs a way back after a logic or schema change, or a corruption, without replaying Kafka.

**Decision.** `make gold-rebuild CONFIRM=1 [TO_TIME=...]` resets gold and loads every silver table in one batch, through the same merge as the stream, up to a `_changed_at` cutoff. It then applies retention (#49, #150).

- A rebuild up to T equals gold streamed from exactly the changes committed up to T, late arrivals included.
- Restarting the gold job replays silver idempotently and continues past the cutoff.

**Why.** Reusing the streaming merge means a rebuild cannot drift from what the stream would have written.

**Alternatives considered.** Restarting the gold job from an empty checkpoint, which has no cutoff and replays every change one micro-batch at a time.

**Outcome.** Infra tests check that a rebuild equals streaming the same changes, with and without a cutoff. Surrogate keys get new values after a rebuild. The rebuild is one of two recovery operations; the other is the bronze replay:

| | Bronze replay (#133) | Gold rebuild (#150) |
| --- | --- | --- |
| Purpose | Fill gaps: re-read a landing range and append only the changes bronze lacks | Recreate gold from scratch from silver |
| Destructive | No, append-only | Yes: `gold-reset` drops and recreates the gold schemas and checkpoints |
| Input | Landing topic and an offset or time range | All of silver, optionally up to `TO_TIME` |
| Deduplication | Anti-join on (key, `_commit_sequence`, `_change_op`); quarantine once per source coordinates | Starts empty; loads through the gold job's own sinks |
| Afterwards | Silver picks the rows up as late events; no restart | The gold job is left stopped; `make pipeline` resumes from fresh checkpoints |
| Side effects | None | Surrogate keys regenerated |
| When | A missed or failed range | A logic or schema change in gold, or corrupted gold |

Both reuse the streaming code and are tested to equal streaming output. A full bronze rebuild is a manual procedure ([rebuilding bronze](../operations/maintenance-and-recovery.md#rebuilding-bronze-from-landing)). Replaying a silver range into gold would be a cheap extension, because the gold writer is idempotent.

### D39 · Gold in the lake as Iceberg views over silver

**Status.** Superseded by [D44](#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy): the gold job writes gold as Iceberg tables in `lake.gold`, and the lake gold views are thin views over them. Kept as the record of the earlier design.

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

**Context.** Gold was computed by SQL in PostgreSQL: each micro-batch was staged and merged by templated SQL, the dimensions' fold flags and validity ranges were derived by views on every read, the surrogate keys were PostgreSQL identities, and the lake had gold only as views over silver ([D39](#d39--gold-in-the-lake-as-iceberg-views-over-silver)). Every other transformation in the pipeline runs in Spark, so gold's rules were written twice, once in PostgreSQL SQL and once in the lake views, and the lake had no stored, columnar copy of the model. Rebuilding gold also gave every dimension version a new key.

**Decision.** The gold job computes the whole gold model with the Spark DataFrame API inside each micro-batch's `foreachBatch`, writes it to Iceberg tables in `lake.gold` with `DataFrame.mergeInto`, and copies what it wrote into PostgreSQL with plain upserts (#221 for the transforms).

- **One batch at a time.** One lock in the driver serialises the gold streams' batches: read the stored rows, compute, write to Iceberg, upsert into PostgreSQL. Dimensions are written before facts, current state before reports. A batch whose Iceberg commit conflicts is replayed, because it is idempotent.
- **SCD2 dimensions per touched key.** A batch recomputes only the business keys it touches: their stored versions plus the batch's, deduplicated on (key, `_commit_sequence`), then window functions partitioned by the business key and ordered by `_commit_sequence` store `_is_folded`, `_version_number`, `_valid_from`, `_valid_to` and `_is_current`. `_commit_sequence` is Debezium's `source.lsn`; `_version_number` counts the versions that are not folded.
- **Deterministic surrogate keys.** `customer_key` and `product_key` are `xxhash64(business key, _commit_sequence)`.
- **History facts** get key lookups (the seeded `dim_order_status` and `dim_date`, broadcast, with the `UNKNOWN` fallback) and as-of lookups (a range join on the stored dimension validity, the newest qualifying version winning, the first version also covering earlier times, folded and deleted versions never matched; `line_amount` is the price as of the change times the quantity). A dimension batch re-resolves the facts of the keys it touched and writes only the rows whose looked-up values change.
- **Current state** applies only newer changes by `_commit_sequence`, keeps deletes soft and stores `is_open`.
- **Reports** are computed every micro-batch: three per-key tables (open orders by delivery date and status; pending orders by customer; pending items by product, as a count and a quantity), recounted only for the keys the batch touched, from both the old and the new current rows, deleting totals that drop to zero; and two top-3 tables ranked with `rank()`, ties for third place kept ([D32](#d32--the-gold-dimensional-model)).
- **Iceberg tables** are format v2, merge-on-read, named `lake.gold.internal_<table>`; the lake gold views are thin views over them.
- **PostgreSQL is a serving copy.** Per batch, one transaction runs generic `INSERT ... ON CONFLICT DO UPDATE` statements, and deletes for removed report rows, rendered from the schemas, with no business logic, and records the Iceberg snapshot ids in `gold_internal.sync_state`. The public `gold` views keep their names and columns; the four business-question views read the report tables. The API and Grafana keep reading PostgreSQL.
- **Rebuild** runs the same transforms over all of silver in one batch, then reloads PostgreSQL from the computed rows. **Retention** stays in PostgreSQL (monthly partitions; rows older than the window are not copied again); Iceberg keeps the full history.

**Why.**

- **One engine for every transformation.** Landing, bronze, silver and now gold are all Spark, tested the same way; gold's rules are written once, in DataFrames, instead of in PostgreSQL SQL and again in the lake views.
- **Columnar storage of the whole model.** The Iceberg tables hold every gold table with its full history, for analytical engines (Spark SQL, DuckDB, Trino), next to silver and maintained like it.
- **Deterministic keys.** Hashing the business key and `_commit_sequence` gives the same key in both copies and after a rebuild, in one narrow column, and keeps pipeline metadata out of the public views. I hash the LSN rather than `_version_number` because version numbers renumber when a late change lands between stored versions or a version unfolds; a version's LSN never changes. Collisions follow the birthday bound over 2^64 values, about 3 in a million at 10 million versions, and PostgreSQL's primary key on the surrogate key rejects one loudly.
- **PostgreSQL only for serving.** It stays what the API and Grafana need, low-latency reads that see each batch whole, without holding the model's logic.
- **Reports instead of a streaming `groupBy`.** A streaming aggregation over a change log cannot take counts back when an order moves to another delivery date or is completed. Recounting the touched keys from the current state is exact, and ranking the top 3 is cheap because it runs over the small per-key table, whose size is the number of keys, not of orders.

**Alternatives considered.**

- Keep gold's SQL in PostgreSQL and add an Iceberg copy: two implementations of the same rules, and the lake copy would still trail PostgreSQL.
- Hash (business key, `_version_number`) for the surrogate keys: renumbered by late changes and unfolds.
- PostgreSQL identities: not reproducible in the lake or after a rebuild.
- A streaming `groupBy` for the reports: cannot retract counts.
- A columnar serving store such as ClickHouse instead of PostgreSQL: the next step for scale ([the way to production](production.md#gold-serving)), not needed at this size.

**Outcome.**

- **Consistency trade-off.** Iceberg commits per table, so a lake reader can briefly see a dimension before its re-resolved facts, or current state before its reports; PostgreSQL commits each batch whole, which is why the business questions are served from it ([consistency](../lakehouse/gold/writer.md#consistency-between-the-two-copies)).
- The PostgreSQL gold indexes chosen for the earlier views ([D40](#d40--gold-indexes-chosen-from-the-query-plans-of-the-current-use-cases)) are no longer needed: the questions read one row per answer.
- **Scaling the reports.** With millions of customers, ranking the whole per-key table every batch grows with it; production would rank a candidate set (the current top N plus the touched keys) with a periodic full re-rank.
- Gold latency after the change: **[pending measurement: end-to-end gold latency p50 / p95 per entity, from the latency rerun]**; gold batch duration: **[pending measurement: gold batch p50 / p95]**.
- Data quality after the change: **[pending measurement: result of a full `make data-quality` run]**.

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
