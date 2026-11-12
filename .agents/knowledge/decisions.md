# Decision log

Local-only record of architecture and process decisions. It is the raw material for M6 (architecture, sizing and evidence): the README's architecture and trade-off sections are written from here. Not tracked in GitHub.

Format per entry: status (`accepted`, `proposed`, `superseded by Dn`), context, decision, alternatives considered, consequences. Append new entries; when a decision changes, mark the old one superseded instead of rewriting history.

## D1 · Two layers: landing zone + gold
- **Status:** superseded by D17 (2026-11-05)
- **Context:** Must-haves are historical + current state, a dimensional model, and data available as soon as possible. The human wants a raw layer to replay or debug data when errors are found.
- **Decision:** A landing zone (raw, append-only Debezium records in the lake on MinIO, full history) and a gold layer (dimensional model in PostgreSQL). No bronze/silver for now.
- **Alternatives:**
  - Full medallion (bronze/silver/gold). Each hop adds a streaming query, schema, checkpoint, recovery path, and commit latency. Silver built with MERGE cannot feed a downstream Spark stream (see L-ICEBERG-STREAM). Deferred to M7 as an experiment.
  - Kafka → Postgres only: no durable replay source beyond Kafka retention.
- **Consequences:** Landing is the system of record for full history; gold can be rebuilt from it. Two streaming hops: Kafka → landing → gold.

## D2 · Gold is built from landing, not directly from Kafka
- **Status:** superseded by D17 (2026-11-05)
- **Context:** The human prefers the usual layer flow, where each layer reads from the previous one.
- **Decision:** Gold streams from the landing Iceberg table.
- **Alternatives:** Two parallel queries from Kafka (lowest latency, but two lineages).
- **Consequences:** One lineage, and rebuild = replay landing. Adds one commit/poll interval of latency. This works only because landing is append-only.

## D3 · Gold store: PostgreSQL, not ClickHouse
- **Status:** accepted (2026-11-05)
- **Context:** The human proposed ClickHouse (columnar, OLAP).
- **Decision:** PostgreSQL for gold.
- **Why:** Gold is small (current state + bounded history + dimensions). The assignment requires "consistent results": Postgres gives transactional MERGE/upserts per micro-batch. ClickHouse CDC (ReplacingMergeTree) deduplicates only eventually, so queries need FINAL/argMax, otherwise open orders can briefly be double-counted. Postgres is also the assignment's suggested destination and is lighter to run.
- **Alternatives:** ClickHouse, a better fit for billions of fact rows, high concurrency, and ad-hoc scans over full history. It can also query Iceberg directly. Tracked in M7.
- **Consequences:** Keep the gold writer and connection behind one module so the store can be swapped.

## D4 · Gold retention: about 3 years in Postgres, full history in the lake
- **Status:** accepted (2026-11-05)
- **Decision:** History facts in Postgres keep a configurable window (default 3 years), partitioned monthly so retention is a partition drop, not a bulk DELETE. Full history stays in landing.
- **Consequences:** Retention must never drop current state for still-open orders whose last change is older than the window.

## D5 · Current-state storage in gold
- **Status:** accepted (2026-11-09, #30): current-state tables updated by the streaming queries, see D32.
- **Human's proposal:** current state as a view or materialized view over the history.
- **Recommendation:** a current-state table updated by Spark in the same transaction as the history append, with the 4 metric views on top.
- **Why:**
  - A materialized view REFRESH recomputes everything (even CONCURRENTLY), which adds latency per micro-batch.
  - A plain "latest version" view slows down as history grows.
  - Both lose old open orders once retention trims history.

## D6 · Serving: Grafana dashboard + FastAPI
- **Status:** accepted (2026-11-05)
- **Decision:** Grafana (dashboard as code, auto-refresh) and a FastAPI metrics API, both reading the gold views.
- **Follow-up:** M7 compares a Go implementation for responsiveness, using measured results only.

## D7 · Spark standalone cluster in Compose
- **Status:** accepted (2026-11-05)
- **Decision:** A Spark master, 2 workers with fixed cores/memory, and the app container as driver.
- **Why:** Makes the sizing section (M6) concrete with real executors, and exposes the master and app UIs for evidence.

## D8 · Stay fully local; no cloud services
- **Status:** accepted (2026-11-05)
- **Why:** Reviewers must run the stack from the README with Docker. The source DB and Debezium are local. MinIO is S3-compatible, so moving to S3 is a configuration change, which is worth stating in the README.
- **If the stack gets too heavy:** keep Conduktor behind its profile, cap worker resources, and reduce to 1 worker before considering cloud.

## D9 · Lake table format: Apache Iceberg with a JDBC catalog on the analytics Postgres
- **Status:** accepted (2026-11-05)
- **Why:** Matches the role's stack (CDC + Spark Structured Streaming + Iceberg). The JDBC catalog avoids an extra catalog service.
- **Alternatives:**
  - REST catalog (closer to production, one more service).
  - Delta Lake (no catalog, diverges from Deel's direction).

## D10 · Debezium decimal handling
- **Status:** superseded by D29 (2026-11-08): the connector is not changed; the decimal is decoded in Spark at bronze (#34).
- **Recommendation:** `decimal.handling.mode=string`, so `unity_price` arrives as a decimal string instead of a base64 VariableScaleDecimal struct. Changing it requires resetting topics and the connector.

## D11 · Optional Conduktor monitor without Cortex
- **Status:** accepted and implemented (commit dd69e57)
- **Decision:** Conduktor Console + its own metadata Postgres behind the `monitoring` Compose profile, on host port 8090. Cortex was removed: topic management, browsing and producing work without it; only historical metrics and alerting are lost.
- **Note:** Console has no embedded database; PostgreSQL 13+ is mandatory.

## D12 · Planning hierarchy
- **Status:** accepted (2026-11-05)
- **Decision:**
  - Milestone = user story.
  - Issue = executable deliverable with objective and acceptance criteria.
  - Tasks are native GitHub sub-issues of that issue, not checklists.
  - Small single-deliverable issues have no sub-issues.
  - Milestone "Further improvements" is an open backlog for experiments.
  - No epics.

## D13 · Object store: SeaweedFS instead of MinIO
- **Status:** accepted (2026-11-05)
- **Context:** MinIO's GitHub repository is archived; its last release was October 2025. Official `minio/minio` images are no longer pullable from Docker Hub or Quay.
- **Decision:** SeaweedFS, an actively maintained, Apache-2.0, S3-compatible store that runs as one all-in-one container, exposing the S3 API to Spark/Iceberg.
- **Alternatives:**
  - `pgsty/minio`: a community fork that works as a drop-in replacement, but has a single small maintainer.
  - RustFS: younger and less proven with Iceberg.
  - Garage: needs a layout init step and covers less of the S3 API.
- **Final report:** include this. The original plan was MinIO, the usual local S3 stand-in. MinIO is no longer maintained (repository archived, last release October 2025, official images no longer pullable), so maintained open-source S3-compatible options were compared (SeaweedFS, `pgsty/minio` fork, RustFS, Garage) and SeaweedFS was chosen.
- **Consequences:** GitHub issues #10, #11, #12, #22 and #61 now say SeaweedFS. The README should still say that any S3-compatible store, or AWS S3 itself, is a configuration change.

## D14 · Spark 4.1.4 with Iceberg 1.11.0
- **Status:** amended 2026-11-05 (issue #10): Iceberg is **1.12.0**, not 1.11.0. 1.12.0 was released 2026-09-30 and also ships Spark runtimes only up to 4.1, so Spark stays on 4.1.4. Pinned jars: `iceberg-spark-runtime-4.1_2.13:1.12.0`, `iceberg-aws-bundle:1.12.0`, `org.postgresql:postgresql:42.7.14`, added to the image with SHA-256 checksums.
- **Context:** The newest Spark releases are 4.2.1 and 4.3.0 (released 2026-10-30). The newest Iceberg, 1.11.0, ships Spark runtimes only up to 4.1. Iceberg hooks into Spark internals, so each Spark minor version needs its own runtime. No `apache/spark` image exists for 4.3.
- **Decision:** One custom image based on `apache/spark:4.1.4-python3`, shared by master, workers and driver, with `iceberg-spark-runtime-4.1_2.13:1.11.0`.
- **Consequences:** Upgrading means changing the image tag and the runtime artifact once Iceberg supports a newer Spark. A shared image guarantees matching driver and worker versions.

## D15 · No schema-migration tool; plain init and idempotent SQL
- **Status:** accepted (2026-11-05)
- **Context:** The project is small. The human considers schema versioning unnecessary for now. In M2 the Iceberg JDBC catalog creates its own tables, and gold needs only an empty schema.
- **Decision:** A Postgres init script creates the databases and schemas. Later gold DDL uses idempotent SQL (`CREATE … IF NOT EXISTS`). A migration tool (dbmate, Flyway, Liquibase) is an improvement tracked in M7 issue #70.
- **Consequences:** Init scripts run only on an empty volume. Non-idempotent schema changes during development need a reset of the analytics volume only, not `make clean` of the whole stack.

## D16 · Delivery: one branch and PR per issue
- **Status:** accepted (2026-11-05)
- **Decision:** Branch `feat/<issue>-<slug>` from `main`, one commit per sub-issue, and one PR per issue. Merge with merge commits to keep history.

## D17 · Layered lake (landing → bronze → silver), gold in PostgreSQL built from silver
- **Status:** accepted (2026-11-05). Supersedes D1 and D2. GitHub planning log #74.
- **Context:** The human wants landing kept exactly as received, for a clear view of the sources. A list of change events is not a usable history, so the lake needs a proper historical layer.
- **Decision:**
  - Landing: raw Avro files (D18).
  - Bronze: typed, append-only Iceberg change tables per source table, with a quarantine table and a schema evolution policy (#75).
  - Silver: append-only versioned history per entity in Iceberg, with valid-to and current state derived in views (#76).
  - Gold: dimensional model in PostgreSQL, built from silver (M5).
- **Why silver is append-only:** Spark cannot stream from Iceberg tables that receive MERGE (L-ICEBERG-STREAM). Keeping silver append-only keeps gold streamable from it.
- **Accepted cost:** four hops (Kafka → landing → bronze → silver → gold). Latency will be measured in M7. Fallback: the fast path from bronze to gold (#87).
- **Consequences:**
  - Milestones are renumbered. The new M4 is "Bronze and silver history in Iceberg". Gold is M5, serving M6, architecture M7, and M8 "Ideas and experiments" (renamed from "Further improvements").
  - #32 and #64 are closed as not planned.

## D18 · Landing format: raw Avro files, one folder per topic, no partitioning
- **Status:** accepted (2026-11-05)
- **Decision:** Each record stores the Kafka key and value as raw bytes, untouched, plus topic, partition, offset and timestamp. The `spark-avro` package goes into the shared image.
- **Alternatives:** JSON lines (readable with `cat`, but larger and lacking a schema); a partitioned Iceberg table (rejected: not "as received").
- **Why Avro:** row-oriented for a write-heavy layer, compact, splittable and self-describing. Raw bytes stay valid if topics later move to Avro with a Schema Registry (#67).
- **Consequences:** Debugging goes through Spark SQL (#24/#25). The Kafka coordinates make replay (#85) and gap checks possible.

## D19 · Gold has two sinks: PostgreSQL (serving) and Iceberg in the lake (optional consumption)
- **Status:** superseded by D39 (2026-11-10); accepted 2026-11-05. The second sink is issue #86, built after the PostgreSQL gold.
- **Decision:** The gold job writes each micro-batch to PostgreSQL and to Iceberg. Both writes are idempotent (MERGE by business key and `lsn`), because no transaction spans both stores. Lake gold keeps the full history; PostgreSQL keeps the retention window.

## D20 · The analytics PostgreSQL is a separate instance; the CDC publication covers only the four tables
- **Status:** accepted (2026-11-05)
- **Decision:** A dedicated analytics instance (#7). In production it would also be separate. Restrict the publication to the four replicated tables (#84) with `publication.autocreate.mode=filtered`.

## D21 · Published issues are refined with a recorded history, never silently
- **Status:** accepted (2026-11-05)
- **Decision:**
  - Every edit to a published issue gets a dated "Refinement" comment saying what changed, why, and linking the planning log.
  - Moved tasks are re-parented (`replace_parent`), not recreated.
  - Superseded issues are closed as not planned, with a comment linking their replacement.
  - Milestone-level changes get an entry in the pinned planning log, #74.
- **Why:** Readers, and the M7 write-up, can follow how the plan evolved.

## D22 · No Spark History Server for now
- **Status:** superseded by D23 (2026-11-05)
- **Decision:** Only the live application UI is exposed: port 4040 on the container running the driver. A two-minute dummy streaming job (`jobs/spark_ui_check.py`) is the default `make spark-submit` app, so the UI can be inspected.
- **Why:** The human considers a history server unnecessary at this stage. It was implemented briefly and then removed from PR #73.
- **Consequences:** Finished or crashed applications leave no UI. Capture M7 evidence (Spark UI screenshots, streaming metrics) while the pipeline runs. Revisit only if post-mortem debugging or evidence capture needs it.

## D23 · Optional Spark History Server; event logs always written
- **Status:** accepted (2026-11-05). Supersedes D22.
- **Context:** The human agreed once it was clear that finished or crashed applications otherwise leave no UI. Losing that would hurt debugging and the M7 evidence. The human also wanted the stack kept light.
- **Decision:**
  - Drivers always write rolling event logs (128 MiB files) to the `spark-events` volume, configured in `docker/spark/spark-defaults.conf` baked into the image.
  - `spark-history` (port 18080) runs only under the `history` Compose profile, started with `make up-history`.
- **Verified:** A job that ran while the history server was down appeared as completed once the server started.
- **Consequences:**
  - The history server's log cleaner runs only while the server is up, so event logs accumulate until the server is started or `make clean` is run.
  - When the driver moves to the app container (#13), mount `spark-events` there too.
  - When SeaweedFS exists, event logs could move to object storage.

## D24 · Optional services are selected with `make up PROFILES=...`
- **Status:** accepted (2026-11-05)
- **Context:** Optional services were multiplying (`up-monitoring`, `up-history`). The human asked for flags such as `make up --history`, but Make parses every `--option` as its own.
- **Decision:**
  - `make up` maps `PROFILES` (spaces or commas; defaults to an exported `COMPOSE_PROFILES`) to Compose's `COMPOSE_PROFILES`.
  - The `up-monitoring` and `up-history` targets are removed.
  - Per-service targets stay where they are useful on their own, e.g. `make spark-history` starts only the history server.
- **Profiles:** `monitoring` (Conduktor), `history` (Spark History Server).

## D25 · Analytics PostgreSQL: postgres 18.6, credentials in .env, catalog in its own database
- **Status:** accepted (2026-11-05), issue #7
- **Decision:** `analytics-db` uses `postgres:18.6` on host port 5433. Credentials (`ANALYTICS_DB_USER/PASSWORD/NAME`) live in the committed `.env` so Spark jobs can reuse them. The init script creates the `gold` schema in `analytics` and a separate `iceberg_catalog` database for the Iceberg JDBC catalog.
- **Why:** a separate catalog database keeps Iceberg's metadata tables out of the serving database and can be reset or moved independently.
- **Consequences:** #10 points the JDBC catalog at `jdbc:postgresql://analytics-db:5432/iceberg_catalog`. Resetting the analytics volume orphans the lake tables from the catalog.

## D26 · SeaweedFS as `weed mini`, bucket created on startup; Iceberg catalog config baked into spark-defaults
- **Status:** accepted (2026-11-05), issue #10
- **Decision:**
  - `seaweedfs` runs `chrislusf/seaweedfs:4.52` as `weed mini -dir=/data -bucket=lake`. `mini` creates the bucket on startup if missing, so the one-off init service in #11's original wording is not needed. The health check waits for `/buckets/lake/` on the filer.
  - S3 credentials: `.env` holds `LAKE_S3_ACCESS_KEY/SECRET_KEY`; Compose maps them to `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY` inside the SeaweedFS container (it creates an admin identity from them). `.env` deliberately avoids `AWS_*` names so a developer's real AWS credentials exported in the shell never leak into the local store through interpolation.
  - Iceberg catalog `lake` (JDBC, metadata in `analytics-db/iceberg_catalog`, warehouse `s3://lake/warehouse`, S3FileIO with path-style access) lives in `docker/spark/spark-defaults.conf`, so every job and shell gets it. Credentials use Spark `${env:VAR}` substitution from the driver container's env (spark-master today; the driver container in #13 needs the same four env vars).
  - `make up` passes `--build`, because Compose reuses an existing local image tag and would otherwise keep running a stale Spark image.
- **Alternatives:** SeaweedFS's built-in Iceberg REST catalog (port 8181 in `mini`) — a candidate for the REST-catalog idea; an init container with `weed shell s3.bucket.create`; credentials passed via `--conf` in the Makefile.

## D27 · One driver container per application run (client mode), removed when the app ends
- **Status:** accepted (2026-11-05), issue #13
- **Decision:** `spark-app` is an on-demand service (own profile, not started by `make up`). `make spark-submit` uses `docker compose run --rm` with the UI port published, so each application gets its own driver container that disappears when it finishes; streaming apps keep theirs while they run. Executors still run on the shared standalone workers.
- **Why:** a driver OOM or restart affects only its own app; per-app logs, UI port and lifecycle; credentials only in the driver container; mirrors EMR cluster mode / Spark-on-Kubernetes driver pods. Standalone cluster deploy mode does not support Python apps, so client mode in a throwaway container is the closest local equivalent.
- **Alternatives:** a long-running `spark-app` service with `exec` (shared memory, shared lifecycle, second UI on an unpublished 4041); keep drivers in `spark-master`.
- **Consequences:** standalone gives the first app all cores unless capped; concurrent apps need `spark.cores.max` per job (sizing for M3–M5 and M7). Executors must reach a per-run driver by IP/hostname, not the service name.
- **Final report:** compare with EMR (Steps, YARN cluster mode, EMR Serverless) and Spark on Kubernetes (driver/executor pods, Spark Operator `SparkApplication`, restart policies): ours isolates drivers per run but shares executors.

## D28 · Small stacked PRs, one per sub-issue
- **Status:** superseded by D41 (2026-11-10); accepted 2026-11-05, refines D16
- **Decision:** One branch and PR per sub-issue (`feat/<n>-<slug>`). A PR that depends on another is stacked on it (its base branch is the dependency's branch) and the stack merges bottom-up; after a merge, the next PR is retargeted to `main`. The top PR of a stack also closes the parent issue.
- **Why:** the user wants smaller, reviewable PRs instead of one PR per issue.

## D29 · The source database and the Debezium connector stay as provided
- **Status:** accepted (2026-11-08)
- **Context:** The assignment asks for "a Spark application that can process streaming events from the provided Debezium topics", says to set up the source database from the pre-set repository with its docker-compose, and to "focus on your analytical pipeline". The only stated freedom on the database side is the destination ("create a schema in the existing DB or instantiate a new one"). Nothing explicitly forbids changing the connector, but the wording treats the source side as given input.
- **Decision:** Do not change `db-scripts/initialize_db_ddl.sql` or `debezium/connectors/finance-db-connector.json`. The pipeline handles the provided format: `unity_price` arrives as Debezium's variable-scale decimal (`{"scale": n, "value": "<base64 unscaled bytes>"}`) and is decoded at bronze (#34); TIMESTAMP(3) arrives as epoch milliseconds and DATE as days since the epoch.
- **Consequences:** #18 closed as not planned; #84 (publication limited to four tables) moved to M8 as a recommendation for the source owners; #17/#19 document the format as provided. If in doubt, the assignment invites questions at data-platform@deel.com.
- **Final report:** list the source-side recommendations (filtered publication, `decimal.handling.mode=string`) as improvement notes.

## D30 · Bronze metadata naming and change identity (hybrid)
- **Status:** accepted (2026-11-08). GitHub: #33, #34, #75, #76, #78, #80, #82, #85; planning log #74.
- **Decision:**
  - Bronze keeps the source coordinates under technology-neutral names, including the redundant topic: `record_source` (topic), `source_partition`, `source_offset`, `published_at` (Kafka timestamp). Change metadata: `change_op` in words (`create`, `update`, `delete`, `snapshot`), `commit_sequence` (Debezium `source.lsn`; named `change_position` until 2026-11-09, renamed by the user), `changed_at` (`source.ts_ms`), `captured_at` (envelope `ts_ms`).
  - The change identity stays in readable columns: entity key + `commit_sequence` + `change_op`. Silver (#82) and replay (#85) deduplicate on it; Debezium re-sends after a restart carry the same identity at a new offset, so Kafka coordinates are lineage, not identity.
  - `content_hash`: SHA-256 of the entity's business columns (audit columns excluded), columns sorted by name, each as name:length:value, null columns left out (an empty string still counts, so null and empty differ, and adding a still-null column changes no hash; updated 2026-11-09), consistent formatting (#80). Silver skips a version whose hash equals the latest one, which absorbs Debezium re-snapshots and audit-only updates.
- **Alternatives:** full Data Vault (hash key per entity, hashed change id): same deduplication power, but opaque ids and normalization drift risk; little value with single integer keys. Dropping coordinates from bronze: lineage would need searches by key and position.
- **Final report:** explain the hybrid and why Data Vault hashing was only partly adopted.

## D31 · Late arrivals are history; deduplication in silver; maintenance must not outrun streams
- **Status:** accepted (2026-11-08). GitHub: #35, #76, #79, #81, #82, #83, #85, #77, #26, #106; planning log #74.
- **Decision:**
  - Bronze is a faithful log of what arrived: deletes kept with the before image, tombstones skipped, no deduplication (Debezium duplicates arrive in different micro-batches anyway).
  - Silver deduplicates by change identity (D30). Events newer than the latest version are skipped when `content_hash` is unchanged (re-snapshots, audit-only updates). **Late events** (older than the latest stored version, e.g. delayed partition or replay) are **appended as history**; `valid_to` is derived in views ordered by `commit_sequence`, so they land in place. Tests cover late arrivals in a later micro-batch, not only replays (user request).
  - ~~Comparison with the latest version per key uses streaming state, not a re-read of silver. Open choice in #82: add pandas/pyarrow to the cluster image for Python stateful APIs, or stay JVM-side.~~ Replaced by D36: exact duplicates dropped on write, unchanged versions folded in the `_history` view.
  - Iceberg maintenance: compaction is safe for streams (replace snapshots are skipped), snapshot expiry retention must exceed the slowest downstream stream's lag and restart window.
- **Consequence for M5:** gold's current-state upsert must apply a version only if it is newer than the stored one, so a late version never overwrites the current state.
- **Final report:** late-arrival semantics and why silver appends them.

## D32 · Gold dimensional model in PostgreSQL
- **Status:** accepted (2026-11-09). GitHub: #27, #28, #29, #30, #31; planning log #74.
- **Decision:**
  - SCD2 dimensions `dim_customer` and `dim_product` (one row per version, one current row per key), seeded `dim_date` and `dim_order_status` (with `is_open`).
  - Append-only history facts `fact_order_history` and `fact_order_item_history`, one row per change, keyed by entity key + `commit_sequence`, partitioned monthly on `changed_at`; `valid_to` derived in views. A delete is a history row with `change_op = 'delete'` (no `is_deleted` column in history).
  - Current state as tables (`current_orders`, `current_order_items`) updated by the streaming queries in the same transaction as the history insert; a version is applied only if its `commit_sequence` is higher than the stored one (D31). Deletes are soft (`is_deleted`) so the stored position still blocks late events from resurrecting a deleted row.
  - Open order = current status other than COMPLETED (UNKNOWN or null counts as open) and not deleted. Pending orders (question 4) = open orders. Pending items = items of open orders, reported as item count and total quantity.
  - Top-3 questions include ties (`FETCH FIRST 3 ROWS WITH TIES`), so more than three rows can come back.
  - Retention: 3 years of history in PostgreSQL (drop monthly partitions); current state and dimensions are never trimmed; the lake gold (#86) keeps full history.
- **Alternatives:** current state as a view (slows down as history grows) or a materialized view (full refresh per micro-batch); both lose open orders older than the retention window. Pending orders as literal status PENDING: inconsistent with questions 1-3.
- **Final report:** the open/pending definitions, ties included in the top 3, and current-state tables with newer-version-only upserts.

## D33 · Pipeline metrics in a separate observability catalog in the lake
- **Status:** proposed (2026-11-09), deferred to #127 (M8) for the user's review; out of #43. GitHub: #43, #127; planning log #74.
- **Decision:** instead of a `pipeline_freshness` table in gold, streaming queries record per-micro-batch stats (query name, batch id, input and written rows, durations, last `commit_sequence`, last event time) in an Iceberg table `observability.pipelines.batch_metrics`, in its own Spark catalog `observability` with its own warehouse path in the lake, not in the data catalog `lake`. Freshness is the latest row per query; the same table can hold more pipeline stats later.
- **Consequences:** readers of freshness (the API's freshness endpoint, #54) read the lake instead of PostgreSQL; the catalog needs its own Spark configuration.
- **Write path (2026-11-09):** start simple, with one metrics row appended per micro-batch inside each query's `foreachBatch` after the data write. Switch to a `StreamingQueryListener` with buffered appends only if measured batch durations (#58) show the extra commit matters; #77 compacts the small files. Live monitoring with Prometheus is M8 #126.
- **Final report:** separating operational metadata (observability/governance) from business data.

## D34 · Metadata columns on lake tables start with `_`
- **Status:** accepted (2026-11-09). GitHub: #75, #76, #31, #86; planning log #74.
- **Decision:** every column the pipeline adds that is not source or business data starts with `_`, so readers can tell metadata from data at a glance (rule 6 in `.agents/README.md`).
  - Bronze and quarantine: `_record_source`, `_source_partition`, `_source_offset`, `_published_at`, `_change_op`, `_commit_sequence`, `_changed_at`, `_captured_at`, `_content_hash`.
  - Silver views: `_commit_sequence`, `_change_op`, `_changed_at`, `_valid_from`, `_valid_to`, `_is_current`, `_is_deleted`, `_content_hash`.
  - Gold (PostgreSQL and the lake copy, which must match): `_commit_sequence`, `_change_op`, `_changed_at`, `_valid_from`, `_valid_to`, `_is_current`, `_is_deleted`; business columns (`status_key`, `delivery_date`, `unit_price`, ...) unchanged.
  - Landing keeps Kafka's own names (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`): its contract is the raw Kafka record, with no source/metadata mix.
- **Final report:** the naming convention and the landing exception.

## D35 · Gold public views over an internal schema
- **Status:** accepted (2026-11-09). GitHub: #129 review, #31.
- **Decision:** the gold tables live in schema `gold_internal`, written only by the gold streaming query. Schema `gold` holds one view per table, with the same name, for consumers (business queries, API, catalog browsing).
  - The views leave out `_commit_sequence` (replay and version ordering) and `_is_deleted` (soft deletes); the current-state views filter deleted rows.
  - The history views absorb the former `*_versions` views: one row per change with `_valid_to` and `_is_current` computed from the next version by `_commit_sequence`.
  - SCD2 validity (`_valid_from`, `_valid_to`, `_is_current`), `_changed_at` and `_change_op` stay public: consumers need them for "as of" queries.
  - `data_freshness` stays a public view; it reads the internal tables, deleted rows included.
- **Alternatives:** exposing the tables with every metadata column (consumers see pipeline internals and must remember to filter deletes); dropping `_commit_sequence` from gold (the writer needs it to stay idempotent and to keep late events from overwriting newer state).
- **Consequences:** a database created before this change has `gold` tables, so the views cannot replace them; recreate the analytics volume. Descriptions are written on both the tables and the views until they are rendered from the shared TOML definitions.
- **Final report:** separating the pipeline's storage contract from the consumer contract.

## D36 · Silver deduplication: exact duplicates dropped on write, unchanged versions folded in the history view
- **Status:** accepted (2026-11-10). GitHub: #82, #83, #76; planning log #74. Replaces the "comparison with the latest version per key uses streaming state" bullet and the open choice of D31.
- **Decision:**
  - Write side: the silver stream runs `dropDuplicates([<entity key>, "_commit_sequence", "_change_op"])` without a watermark, so a redelivered or replayed change is never appended twice and a late change is never dropped. The state is JVM-side, on the RocksDB state store provider, and the output goes through the exactly-once Iceberg streaming sink.
  - `lake.silver.<entity>` stays insert-only and keeps unchanged versions (re-snapshots, audit-only updates) as rows of their own: a hash diff, Data Vault style.
  - Read side (`lake.silver.<entity>_history`): ordered by `_commit_sequence` per key, a version folds into the previous one when its `_content_hash` and `_is_deleted` both equal the previous version's. The folded rows are filtered out first, then `lead()` over the remaining versions gives `_valid_to`. `_is_deleted` is part of the rule because a delete keeps the last known values, so its hash equals its predecessor's; the same holds for a re-create after a delete with identical values.
  - The "exactly one version row per change" criterion applies to the `_history` view, not the table.
  - Gold streams the table, so its writer applies the same fold rule; the rule is written once in the silver → gold contract.
- **Alternatives:** Python `transformWithState` (needs pandas, pyarrow and protobuf on the executors, state calls cross a local socket to the JVM); a `foreachBatch` lookup of silver's latest versions (re-reads silver every micro-batch and needs a hand-written idempotency guard).
- **Consequences:** deduplication state grows by one small entry per change and is never cleaned (no watermark); adding it changes the query's state, so silver checkpoints and tables written before it must be reset. The table holds more rows than the view.
- **Final report:** hash-diff silver, write-side vs read-side deduplication, and why late events need no special handling.

## D37 · Gold retention: back-partition the default partition, then drop whole months
- **Status:** accepted (2026-11-10). GitHub: #48, PR #149.
- **Decision:** `gold_internal.apply_history_retention(keep_months, months_ahead, as_of)` (plpgsql in the gold DDL) first gives every month the default partition holds a partition of its own (plus the current month and `months_ahead`), then drops the monthly partitions whose upper bound is on or before `date_trunc('month', as_of) - keep_months`; a partly-outside month is kept. `pipelines/gold/retention.py` calls it in one transaction after the gold writer lock. Scheduled by `gold-retention-job` in the `maintenance` profile (shell loop, `GOLD_RETENTION_INTERVAL_HOURS`, default 24); `make gold-retention` for one-off runs. Defaults: `GOLD_RETENTION_MONTHS=36`, `GOLD_PARTITION_MONTHS_AHEAD=2`.
- **Alternatives:** Python planning the drops (needs fetch inside the transaction, parses bounds from names); `DELETE` from the default partition (row deletes instead of partition drops); leaving old default rows (breaks the window after any reload).
- **Consequences:** plain DETACH/DROP takes a brief exclusive lock on the fact table (CONCURRENTLY is not allowed with a default partition). Lock order is advisory lock, then table locks, for every gold writer.
- **Final report:** partition-based retention and why the default partition needs back-partitioning.

## D38 · Gold rebuild from silver: batch job through the streaming sinks, cutoff on `_changed_at`
- **Status:** accepted (2026-11-10). GitHub: #49, #47, PR #150.
- **Decision:** `make gold-rebuild CONFIRM=1 [TO_TIME=...]` reuses `make gold-reset`, then `pipelines/gold/rebuild.py` reads each silver table in batch, keeps `_changed_at <= TO_TIME` (ISO 8601, inclusive, UTC by default), writes one batch per load through the gold job's `MergeSink` (dimensions first; each merge under the writer lock), then applies retention. The gold job is left stopped; restarting it on fresh checkpoints replays silver idempotently and continues past the cutoff.
- **Alternatives:** cutoff on `_commit_sequence` (exact but not human-friendly); auto-restarting the gold job (passes the cutoff immediately); handing the stream the rebuild's silver snapshot (saves the replay, adds complexity).
- **Consequences:** surrogate keys get new values after a rebuild. A rebuild up to T equals gold streamed from exactly the changes committed up to T, late arrivals included (infra-tested).
- **Final report:** rebuildability from silver and the equivalence test.

## D39 · Gold in the lake as Iceberg views over silver, with natural keys
- **Status:** accepted (2026-11-10). GitHub: #86 (rewritten), planning log #74. Supersedes D19.
- **Decision:** no second gold sink. `lake.gold.<relation>` holds one Iceberg view per public gold relation, computed at read time from the silver tables and their `_history` views; `make lake-views` creates them after the silver views. The gold job writes PostgreSQL only.
  - Same rules as PostgreSQL gold: D36 fold (from silver's history views), SCD2 validity, as-of attributes with the first-version rule and the highest `_commit_sequence` winning on overlap, current state = latest change by `_commit_sequence` folded or not, soft deletes hidden, `UNKNOWN` status fallback, date keys.
  - No surrogate keys in the lake (PostgreSQL identities cannot be reproduced): facts carry natural keys plus the as-of attributes (`unit_price`, `line_amount`); dimensions expose `_valid_from`/`_valid_to`/`_is_current` for point-in-time joins. No version column in public views.
  - `dim_date` and `dim_order_status` are generated in the lake templates; an infra test checks them against PostgreSQL's seeds.
  - The SQL is rendered from the same declarations as the PostgreSQL writer (`pipelines/gold/dimensions.py`) and the gold Avro schemas.
- **Alternatives rejected:** two sinks with MERGE-maintained Iceberg tables (duplicates the incremental SQL in Spark, cross-stream Iceberg conflicts); mirroring PostgreSQL into the lake (a CDC-like flow out of the analytics DB, rejected by the user); append-only lake change tables (duplicates silver).
- **Consequences:** lake reads recompute over the full history; materialize later if needed. The PostgreSQL ↔ lake reconciliation (`CHECK=gold-lake`) matches versions internally through silver's `_commit_sequence`.
- **Final report:** why gold in the lake is views, natural keys versus surrogate keys.

## D40 · Gold indexes chosen from the query plans of the current use cases
- **Status:** accepted (2026-11-10). GitHub: #44, #45, PR #142 (commit b748898).
- **Decision:** the gold query indexes exist only for the current use cases, the four business-question views and `gold.open_orders`, and each one was chosen by reading that query's EXPLAIN plan.
  - `current_orders` stores `is_open` (user decision), so the planner sees how few orders are open.
  - Partial indexes `current_orders_open_delivery_idx (delivery_date, status_key)` and `current_orders_open_customer_idx (customer_id)`, both `WHERE is_open AND NOT _is_deleted`: the delivery-date and top-customer views become index-only scans.
  - `current_order_items_live_order_idx (order_id) INCLUDE (product_id, quantity) WHERE NOT _is_deleted`: pending items looks up the items of each open order.
  - Four current-state indexes no plan used were dropped: `current_orders_status_delivery_idx`, `current_orders_customer_idx`, `current_order_items_order_idx`, `current_order_items_product_idx`.
- **Evidence (#142, scratch postgres:18.6, 1M orders about 3% open, 3M items, 50k customers, three runs):** open orders by delivery date 31–33 → 8–10 ms; top delivery dates 30–35 → 3–4 ms; pending items 344–380 → 37–46 ms; top customers 16–21 → 12–13 ms.
- **Consequences:** a new use case gets its index the same way: write the query, read its plan on representative data, add what the plan uses, drop what no plan uses. #58 measures the same queries on the real run.
- **Final report:** in `docs/architecture/decisions.md` (D40) and the "PostgreSQL query cost" part of `docs/architecture/sizing.md`.

## D41 · One native GitHub stack per line of work; the agent is the orchestrator
- **Status:** accepted (2026-11-10). Supersedes D28.
- **Decision:** PRs that depend on each other or share a line of work go into one native GitHub stack (`gh stack`), one stack per line of work. Stacked PRs merge only through `gh stack merge <pr> --yes --merge`, which merges that PR and everything below it atomically; native stacks retarget the rest automatically. Restacks start with a fetch and a `git range-diff` against the remote head and push with `--force-with-lease=<branch>:<expected>`. The agent working with the user is the orchestrator and delegates to subagents in their own worktrees; parallel worker sessions are no longer used. The agent chooses commit messages (Conventional Commits, no ticket IDs, no AI attribution).
- **Why:** hand-chained bases drifted after the user's restacks, `gh pr merge` refuses stacked PRs, and coordinating parallel sessions that cannot message each other cost more than one orchestrator with subagents.
- **Consequences:** rules live in `.agents/rules/` (`git-and-prs.md`, `collaboration.md`), with skills in `.agents/skills/`. A base change does not trigger CI, so PRs are closed and reopened after a restack.

## D42 · Spark metrics: Spark's Prometheus servlet, scraped by DNS service discovery
- **Status:** accepted (2026-11-11). GitHub: #69, #126, PRs #205, #206, #207.
- **Decision:** every Spark process serves its metrics through Spark's `PrometheusServlet` sink; an optional Prometheus (`metrics` profile) scrapes them every 10 s and keeps 30 days in a volume. Drivers are found by their Compose service names through Docker's DNS (`make spark-submit` drivers answer to `spark-app`), and the scrape configuration rewrites the query and application names Spark puts in metric names into labels. The stage and task details the servlet lacks come from a small exporter that reads the History Server's REST API; the same exporter measures the lake's Iceberg tables (data files, sizes, small files) with PyIceberg through the read-only REST catalog.
  - Grafana (user decision): the Gold dashboard stays alone in its **Gold** folder, as before; the Spark and Iceberg tables dashboards go into one **Spark and lake** folder, one named provider per folder, linked to each other by a links bar on the shared `spark-and-lake` tag.
- **Alternatives:** the JMX exporter as a Java agent (one more jar and port per JVM, same metrics under other names); a Pushgateway (keeps a dead driver's last values); Prometheus's Docker service discovery (needs the Docker socket, root on the host); per-stage metrics from a `SparkListener` in every driver (a JVM plugin, lost with the driver, while the History Server already has every stage); folders from the dashboards' directory names (lowercase folder names, and the Gold dashboard leaves its folder).
- **Consequences:** a driver nobody scrapes pays nothing; without the profile the Spark dashboards stay empty and Gold is unaffected. The Gold provider points at its file, not the directory, because Grafana walks provider directories recursively.
- **Final report:** in `docs/architecture/decisions.md` (D42) and `docs/operations/monitoring.md`.
