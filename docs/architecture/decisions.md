# Design decisions

The design decisions a reviewer should know about, with the alternatives that were weighed. The numbers (D3, D13, ...) are the project's decision log numbers; other pages cite them. The issues and pull requests linked under each decision hold the discussion and the change.

## Platform

### D8 · Fully local, no cloud services

- **Decision:** everything runs in Docker Compose on one machine, so reviewers can run the stack from the README.
- **Consequence:** the source, Kafka, the lake and the serving database are all local. The object store speaks S3 and the catalog is standard Iceberg, so moving to AWS S3 is a configuration change.

### D13 · Object store: SeaweedFS instead of MinIO

- **Decision:** SeaweedFS, running as a single `weed mini` container that exposes the S3 API (#10).
- **Why:** MinIO, the usual local stand-in for S3, is no longer maintained: its repository is archived, its last release was in October 2025, and its official images can no longer be pulled. SeaweedFS is actively maintained, Apache-2.0 licensed, and runs as one container.
- **Alternatives:**
  - the `pgsty/minio` community fork: a drop-in replacement, but with a single small maintainer;
  - RustFS: younger and less proven with Iceberg;
  - Garage: needs a layout init step and covers less of the S3 API.

### D9 · Iceberg with a JDBC catalog on the analytics PostgreSQL

- **Decision:** the lake tables are Apache Iceberg. Their catalog is Iceberg's JDBC catalog, in its own `iceberg_catalog` database on the analytics PostgreSQL.
- **Why:** it is the table format of the target stack, and the JDBC catalog avoids running another service.
- **Alternatives:**
  - a REST catalog: closer to production, but one more service;
  - Delta Lake.

### D14 · Spark 4.1.4 with Iceberg 1.12.0

- **Decision:** one custom image based on `apache/spark:4.1.4-python3` runs the master, the workers and every driver. The Iceberg, AWS bundle and PostgreSQL JDBC jars are pinned with checksums.
- **Why:** Iceberg ships a runtime per Spark minor version, and its newest runtime targets Spark 4.1. A shared image guarantees that the driver and executor versions match.

### D27 · One driver container per application, executors on shared workers

- **Decision:** a Spark standalone master with two workers (D7). Every application gets a throwaway driver container in client mode: `make spark-submit`, one-off jobs, and one service per streaming job. Its executors run on the shared workers, and each application is capped with `spark.cores.max` (#13).
- **Why:** a driver failure, its memory, its logs and its UI port belong to one application only. Standalone cluster mode does not support Python applications, so client mode in a per-run container is the closest local equivalent of a driver pod.
- **In production:**
  - On EMR, a step in YARN cluster mode runs the driver inside the cluster, or EMR Serverless runs the job.
  - On Kubernetes, the Spark Operator gives each application its own driver and executor pods, with restart policies.

  The local setup isolates drivers but shares executors.

### D23 · Event logs always written, optional history server

- **Decision:** every driver writes rolling event logs (128 MiB files) to a shared volume. The Spark History Server runs only under the `history` profile, which `make pipeline` starts.
- **Why:** finished or crashed applications otherwise leave no UI to debug or to take evidence from.

### D3 · Gold in PostgreSQL, not ClickHouse

- **Decision:** the dimensional model is served from PostgreSQL.
- **Why:**
  - Gold is small: current state, bounded history and dimensions.
  - The assignment asks for consistent results. PostgreSQL applies each micro-batch in one transaction, while ClickHouse's CDC tables deduplicate only eventually, so open orders could be double-counted for a while.
- **Alternative:** ClickHouse, which fits billions of fact rows, high query concurrency and scans over the full history better. It stays a candidate for scale.

### D20 · A separate analytics PostgreSQL

- **Decision:** gold and the Iceberg catalog live in their own PostgreSQL instance (`analytics-db`), never in the source database. In production they would be separate too.

## Source side

### D29 · The source database and the Debezium connector stay as provided

- **Decision:** the pipeline handles the format the supplied connector produces. It does not change the source DDL or the connector.
  - `unity_price` arrives as Debezium's variable-scale decimal and is decoded at bronze (#34).
  - Timestamps arrive as epoch milliseconds, and dates as days since the epoch.
- **Why:** the assignment treats the source side as given and asks for the analytical pipeline.
- **Recommendations to the source owners:**
  - `decimal.handling.mode=string`;
  - a publication limited to the four replicated tables (#84).

## Lake layers

### D17 · Layered lake: landing, bronze, silver; gold built from silver

- **Decision:**
  - **Landing** keeps the records exactly as Kafka delivered them.
  - **Bronze** keeps typed, append-only change tables, with a quarantine table for records that fail validation.
  - **Silver** keeps append-only version history per entity.
  - **Gold** builds the dimensional model from silver.
- **Why silver is append-only:** Spark cannot stream from an Iceberg table that receives MERGE or deletes. An append-only silver keeps gold streamable.
- **Cost:** four streaming hops, each with its own checkpoint and commit latency. A fast path from bronze to gold (#87) is the fallback if latency matters.

### D18 · Landing as raw Avro files, one folder per topic

- **Decision:** each record keeps the Kafka key and value as raw bytes, plus topic, partition, offset and timestamp. The records are stored in Avro files with no partition folders (#21, #22).
- **Why:**
  - Avro is row-oriented, compact and self-describing, which suits a write-heavy layer.
  - Raw bytes stay valid if the topics later move to Avro with a schema registry.
  - The Kafka coordinates make replays and gap checks possible.

### D30 · Change identity in readable columns, Data Vault hashing only in part

- **Decision:**
  - Bronze keeps the source coordinates under technology-neutral names (`_record_source`, `_source_partition`, `_source_offset`, `_published_at`) and the change metadata (`_change_op` in words, `_commit_sequence` from Debezium's `source.lsn`, `_changed_at`, `_captured_at`).
  - A change is identified by entity key + `_commit_sequence` + `_change_op`. Debezium re-sends a change after a restart at a new offset, so the Kafka coordinates are lineage, not identity.
  - `_content_hash` is a SHA-256 of the business columns, like Data Vault's hash diff. Silver uses it to recognise versions that change nothing, such as re-snapshots and audit-only updates.
- **Alternative:** full Data Vault hash keys. They deduplicate just as well, but give opaque ids and add normalisation risk, for little gain with single integer keys.

### D34 · Metadata columns start with `_`

- **Decision:** every column the pipeline adds to a lake table starts with `_`, so readers can tell metadata from source data at a glance. Landing keeps Kafka's own column names, because its contract is the raw Kafka record.

### D31 · Late arrivals are history, deduplication happens in silver

- **Decision:**
  - Bronze is a faithful log: deletes keep their before image, tombstones are skipped, and nothing is deduplicated.
  - Silver drops repeated deliveries of the same change. A late change, older than versions already stored, is appended as history: `_valid_to` is derived in the views ordered by `_commit_sequence`, so it lands in its place.
  - Gold's current state applies a version only if it is newer than the stored one, so a late version never overwrites the current state.
  - Lake maintenance must not outrun the streams. Compaction is safe because streams skip `replace` snapshots. Snapshot expiry checks every downstream stream's checkpoint first and skips a table whose stream still needs a snapshot (#135).

### D36 · Silver deduplication: exact duplicates on write, unchanged versions on read

- **Decision:**
  - **On write:** the silver stream runs `dropDuplicates` on the change identity, without a watermark and with RocksDB state. A redelivered or replayed change is appended once, and a late change is never dropped (#82).
  - **On read:** the `_history` views fold a version into the previous one when its `_content_hash` and `_is_deleted` both match, then derive the validity range. Gold applies the same rule (#136, #137).
- **Alternatives:**
  - Python `transformWithState`: needs pandas, pyarrow and protobuf on the executors, and every state call crosses a socket to the JVM;
  - a per-batch lookup of silver's latest versions: re-reads silver every batch and needs its own idempotency guard.
- **Consequence:** the state grows by one small entry per change and is never cleaned, and the silver table holds more rows than its history view.

## Gold

### D32 · The gold dimensional model

- **Decision:**
  - **Dimensions:** `dim_customer` and `dim_product` are SCD2, with one row per version and one current row per key. `dim_date` and `dim_order_status` are seeded.
  - **History facts:** append-only, one row per change, partitioned by month.
  - **Current state:** tables updated in the same transaction, applying only newer versions. Deletes are soft, so a late event cannot bring a deleted row back.
  - **Open order:** any status other than `COMPLETED`, `UNKNOWN` and null included. Pending orders are the open orders, and pending items are the items of open orders, reported as a count and a quantity.
  - **Top 3:** both top-3 questions keep ties for third place, so they can return more than three rows.
- **Alternatives:**
  - current state as a view or materialized view: it slows down as history grows, refreshes in full, and loses old open orders once retention trims history;
  - pending meaning the literal status `PENDING`: inconsistent with the other questions.
- **Details:** [the gold model](gold-model.md).

### D35 · Public views over an internal schema

- **Decision:** the pipeline writes tables in `gold_internal`, and consumers read views of the same names in `gold`.
  - The views leave out pipeline metadata (`_commit_sequence`) and soft-deleted rows.
  - They keep the SCD2 validity columns and the change time and type that "as of" queries need.
- **Why:** it keeps the pipeline's storage contract apart from the consumer contract. The writer still needs `_commit_sequence` to stay idempotent and to keep late events from overwriting newer state.

### D40 · Gold indexes chosen from the query plans of the current use cases

- **Decision:** the gold query indexes exist only for the current use cases: the four business-question views and `gold.open_orders`, the view that holds the open-order rule. Each index was chosen by reading that query's EXPLAIN plan (#44, #45, #142).
  - **`is_open` is stored** on `gold_internal.current_orders`, from `dim_order_status` with the `UNKNOWN` fallback. It is recomputed on every insert and on every newer-version update. This lets the planner see how few orders are open.
  - **Partial indexes** on `current_orders`, both `WHERE is_open AND NOT _is_deleted`. They hold only open orders and make the views that read them index-only scans:
    - `current_orders_open_delivery_idx (delivery_date, status_key)` for the two delivery-date views;
    - `current_orders_open_customer_idx (customer_id)` for the top customers.
  - **`current_order_items_live_order_idx`** `(order_id) INCLUDE (product_id, quantity) WHERE NOT _is_deleted`: with `is_open` stored, pending items finds the few open orders first, then looks up their items through this index.
  - **Dropped:** four current-state indexes that no plan used: `current_orders_status_delivery_idx`, `current_orders_customer_idx`, `current_order_items_order_idx` and `current_order_items_product_idx`.
  - **Writer indexes** serve the gold writer, not the queries: the unique current-row indexes on the dimensions, and the fact indexes on `customer_id` and `product_id` used to re-resolve dimension keys.
- **Evidence:** EXPLAIN on a scratch `postgres:18.6` with 1M orders (about 3% open), 3M items and 50k customers, after `VACUUM ANALYZE`. Ranges are over three runs, as reported in #142.

  | View | Before (no `is_open`) | After |
  | --- | --- | --- |
  | `open_orders_by_delivery_date_and_status` | Parallel Index Only Scan of all live orders + Hash Join, 31–33 ms | Index Only Scan of the open orders (`current_orders_open_delivery_idx`) + Hash Join, 8–10 ms |
  | `top_delivery_dates_by_open_orders` | Parallel Index Only Scan of all live orders + Hash Join, 30–35 ms | Index Only Scan of the open orders (`current_orders_open_delivery_idx`), 3–4 ms |
  | `pending_items_by_product` | Parallel Hash Join over Seq Scans of `current_order_items` and `current_orders`, 344–380 ms | Nested Loop: Bitmap Heap Scan of the open orders → Index Only Scan of their items (`current_order_items_live_order_idx`), 37–46 ms |
  | `top_customers_by_pending_orders` | Nested Loop over the open statuses, Index Only Scan per status, 16–21 ms | Index Only Scan of the open orders (`current_orders_open_customer_idx`), 12–13 ms |

  - With `rank()`, both top-3 views still read their partial index with an index-only scan, and PostgreSQL stops ranking early (`Run Condition: rank() <= 3`).
  - The plans keep these indexes with a stale visibility map: 10% of rows updated and no vacuum.
- **New use cases** get their indexes the same way: write the query, read its plan on representative data, add the index the plan uses, and drop the indexes no plan uses any more.

### D37 · Gold retention by monthly partitions

- **Decision:** the history facts keep 36 months (`GOLD_RETENTION_MONTHS`), on a schedule and on demand (#48, #149).
  - One PostgreSQL function first gives every month in the default partition a partition of its own.
  - It then drops whole monthly partitions older than the window.
  - Current state and dimensions are never trimmed, and the lake keeps the full history.
- **Alternatives:** row `DELETE`s, or leaving old rows in the default partition, which breaks the window after a reload.

### D38 · Rebuild gold from silver, with an optional cutoff

- **Decision:** `make gold-rebuild CONFIRM=1 [TO_TIME=...]` resets gold and loads every silver table in one batch, through the same merge as the stream, up to a `_changed_at` cutoff. It then applies retention (#49, #150).
  - A rebuild up to T equals gold streamed from exactly the changes committed up to T, late arrivals included.
  - Restarting the gold job replays silver idempotently and continues past the cutoff.
- **Consequence:** surrogate keys get new values after a rebuild.

### D39 · Gold in the lake as Iceberg views over silver

- **Decision:** `lake.gold` holds one Iceberg view per public gold relation. Each view is computed at read time from silver, with the same rules as PostgreSQL gold. The gold job writes PostgreSQL only (#86, #152).
  - The lake uses natural keys, because PostgreSQL's surrogate keys cannot be reproduced there.
  - A data quality check that compares both sides at the silver snapshots gold committed is in progress (#154).
- **Alternatives:**
  - a second sink with MERGE-maintained Iceberg tables: duplicates the incremental SQL and adds cross-stream conflicts;
  - mirroring PostgreSQL into the lake: a CDC-like flow out of the analytics database;
  - append-only lake change tables: duplicates silver.
- **Consequence:** lake reads recompute over the full history. They can be materialised later if needed.

## Observability

### D33 · Pipeline metrics in a separate observability catalog (proposed)

- **Proposal:** each streaming query records per-batch stats in an Iceberg table, in its own `observability` catalog in the lake, apart from business data (#127): row counts, durations, the last `_commit_sequence` and the last event time. Freshness is then the latest row per query.
- **Status:** deferred to the ideas milestone. Gold's `data_freshness` view covers freshness meanwhile.
