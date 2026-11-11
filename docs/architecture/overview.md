# Architecture overview

The pipeline turns the change stream of the supplied `finance_db` PostgreSQL database into a history-keeping lake and a dimensional model for serving.

1. Debezium captures every change of the four source tables into Kafka.
2. Spark Structured Streaming lands the records as raw files.
3. Spark types them in bronze and versions them in silver, both Iceberg tables on SeaweedFS.
4. Spark loads gold, a star schema in a separate analytics PostgreSQL.
5. The lake also exposes the same gold model as Iceberg views over silver.

Everything runs locally with Docker Compose.

> **In progress.** Shown dotted and grey in the diagrams:
> - serving: the Grafana dashboard (#165, #166);
> - the data quality checks `silver` (#147), `gold` (#46) and `gold-lake` (#154), and their schedule (#148);
> - lake maintenance refreshing the views.

## Components

Every Compose service, grouped by the profile that starts it. Services without a profile start with `make up`. `make pipeline` adds the `pipeline` and `history` profiles, and `make up PROFILES=...` adds the optional ones.

```mermaid
flowchart LR
    subgraph source["Source, as supplied"]
        tdb[("transactions-db<br/>PostgreSQL finance_db")]
        connect["kafka-connect<br/>Debezium"]
        dinit["debezium-init"]
        zk["zookeeper"]
        kafka[["kafka"]]
    end

    subgraph drivers["Spark drivers, one container per application"]
        subgraph pipeline["profile pipeline"]
            landing["landing-job"]
            bronze["bronze-job"]
            silver["silver-job"]
            views["lake-views, one-shot"]
            gold["gold-job"]
        end
        subgraph scheduled["profile maintenance"]
            maint["maintenance-job"]
        end
        subgraph adhoc["profile app"]
            app["spark-app: spark-submit, spark-sql,<br/>data-quality, bronze-replay,<br/>gold-rebuild, lake-maintenance"]
        end
    end

    subgraph cluster["Spark standalone cluster"]
        master["spark-master"]
        w1["spark-worker-1<br/>5 cores, 5 GiB"]
        w2["spark-worker-2<br/>5 cores, 5 GiB"]
    end

    subgraph storage["Lake and analytics storage"]
        s3[("seaweedfs<br/>bucket lake")]
        adb[("analytics-db<br/>analytics + iceberg_catalog")]
    end

    retention["gold-retention-job<br/>profile maintenance, Python"]
    history["spark-history<br/>profile history"]

    subgraph monitoring["profile monitoring"]
        conduktor["conduktor-console"]
        cdb[("conduktor-db")]
    end

    subgraph serving["Serving"]
        grafana["Grafana"]
        api["api: analytics API<br/>FastAPI"]
    end

    tdb -- "logical replication" --> connect
    dinit -- "registers the connector" --> connect
    connect --> kafka
    zk --- kafka
    kafka -- "CDC topics" --> landing

    drivers -. "executors" .-> master
    master --- w1
    master --- w2
    drivers -- "files, Iceberg data,<br/>checkpoints" --> s3
    drivers -- "Iceberg catalog,<br/>gold over JDBC" --> adb
    drivers -. "event logs" .-> history
    retention --> adb

    conduktor --> kafka
    conduktor --- cdb
    serving --> adb

    classDef progress stroke-dasharray: 2 3,color:#888,stroke:#888
    class grafana progress
```

| Service | Profile | Role | Host port |
| --- | --- | --- | --- |
| `transactions-db` | default | Supplied source database `finance_db`, with logical replication | 5432 |
| `zookeeper` | default | Kafka coordination | 2181 |
| `kafka` | default | One broker. Holds the four CDC topics `finance_db.operations.{customers,products,orders,order_items}` | 9092 |
| `kafka-connect` | default | Debezium PostgreSQL connector, as supplied | 8083 |
| `debezium-init` | default | One-shot: registers the supplied connector | none |
| `api` | default | The analytics API (FastAPI): the four business questions and the freshness of gold, read from the `gold` views as the read-only login `api_reader` | 8000 |
| `api-dev` | `app` | Image for the API's own checks (`make test`, `make typecheck`) | none |
| `seaweedfs` | default | S3-compatible object store. Bucket `lake` holds landing files, Iceberg data and metadata, checkpoints and quality reports | 8333 (S3), 8888 (filer) |
| `analytics-db` | default | Analytics PostgreSQL. Database `analytics` holds gold (`gold_internal`, `gold`), and database `iceberg_catalog` holds the Iceberg JDBC catalog. Consumers log in with read-only service logins in the `gold_reader` role (`make db-logins`) | 5433 |
| `spark-master` | default | Standalone master | 8080 |
| `spark-worker-1`, `spark-worker-2` | default | Executors, 5 cores and 5 GiB each | 8081, 8082 |
| `landing-job` | `pipeline` | Kafka to raw Avro landing files | 4040 |
| `bronze-job` | `pipeline` | Landing to typed bronze and quarantine tables | 4041 |
| `silver-job` | `pipeline` | Bronze to deduplicated silver version tables | 4042 |
| `lake-views` | `pipeline` | One-shot: creates or updates the silver views and the lake gold views | none |
| `gold-job` | `pipeline` | Silver to the PostgreSQL gold model | 4045 |
| `maintenance-job` | `maintenance` | Compacts, expires and cleans the lake's Iceberg tables on a schedule | 4044 |
| `gold-retention-job` | `maintenance` | Drops gold history months outside the retention window on a schedule (Python, no Spark) | none |
| `spark-app` | `app` | Image for throwaway drivers started by `make spark-submit`, `make spark-sql`, `make data-quality`, `make bronze-replay` (UI 4043), `make gold-rebuild` and `make lake-maintenance` | `SPARK_UI_PORT` |
| `spark-history` | `history` | Spark History Server over the shared `spark-events` volume | 18080 |
| `conduktor-console`, `conduktor-db` | `monitoring` | Optional Kafka console and its metadata database | 8090 |

Each job's driver runs in its own container in client mode, and its executors run on the shared workers (decision [D27](decisions.md#d27--one-driver-container-per-application-executors-on-shared-workers)). Each application is capped at `spark.cores.max=2`, so the four streaming jobs use 8 of the 10 cores and leave 2 for ad-hoc runs and maintenance. [The sizing strategy](sizing.md) explains how the cores and memory are shared.

## Data flow

```mermaid
flowchart LR
    src[("finance_db.operations<br/>customers, products,<br/>orders, order_items")]
    topics[["Kafka topics<br/>finance_db.operations.*"]]
    landing[/"Landing<br/>raw Avro files<br/>s3a://lake/landing/TOPIC"/]
    bronze[("Bronze<br/>lake.bronze.ENTITY")]
    quarantine[("Quarantine<br/>lake.bronze.ENTITY_quarantine")]
    silver[("Silver<br/>lake.silver.ENTITY")]
    sviews["Silver views<br/>ENTITY_history, ENTITY_current"]
    stage[("gold_internal.stage_*<br/>unlogged staging")]
    gint[("gold_internal<br/>dimensions, history facts,<br/>current state")]
    gpub["gold views<br/>dimensions, facts, current state,<br/>open_orders, business questions,<br/>data_freshness"]
    lgold["lake.gold views<br/>same relations, natural keys"]
    consumers["Consumers<br/>psql, Spark SQL,<br/>Grafana, FastAPI"]

    src -- "Debezium" --> topics
    topics -- "landing-job" --> landing
    landing -- "bronze-job<br/>parse, type, validate" --> bronze
    landing -- "bronze-job<br/>failed validation" --> quarantine
    bronze -- "silver-job<br/>dropDuplicates on change identity" --> silver
    silver -- "lake-views" --> sviews
    silver -- "gold-job<br/>Spark JDBC" --> stage
    stage -- "one transaction<br/>under the writer lock" --> gint
    gint --> gpub
    sviews -- "lake-views" --> lgold
    gpub --> consumers
    lgold --> consumers

```

| Layer | Where | Written by | What it guarantees |
| --- | --- | --- | --- |
| Landing | Avro files, one folder per topic, `s3a://lake/landing/<topic>/` | `landing-job`, one query per topic | Every Kafka record stored unchanged (key and value as raw bytes, plus topic, partition, offset and timestamp). The file sink and checkpoint make it exactly once, and it is the replay source for bronze |
| Bronze | Iceberg `lake.bronze.<entity>`, partitioned by `days(_published_at)` | `bronze-job`, two queries per topic | A typed, faithful log: one row per change event, deletes with their before image, no deduplication. Records that fail validation go to `<entity>_quarantine` with their landing file. Append-only |
| Silver | Iceberg `lake.silver.<entity>`, partitioned by `days(_changed_at)`, plus the views `<entity>_history` and `<entity>_current` | `silver-job`, one query per topic | One row per change identity (entity key, `_commit_sequence`, `_change_op`), deduplicated with RocksDB state and no watermark, so late changes are appended as history. The views fold versions that change nothing (decision [D36](decisions.md#d36--silver-deduplication-exact-duplicates-on-write-unchanged-versions-on-read)). Append-only, so gold can stream it |
| Gold (PostgreSQL) | Database `analytics`: tables in `gold_internal`, views in `gold` | `gold-job`, one query per silver table | SCD2 dimensions, append-only history facts partitioned by month, and current-state tables that only apply newer versions. Writes are idempotent per micro-batch and serialised by an advisory writer lock. Consumers read the `gold` views, which hide pipeline metadata and soft deletes (decisions [D32](decisions.md#d32--the-gold-dimensional-model), [D35](decisions.md#d35--public-views-over-an-internal-schema)) |
| Gold (lake) | Iceberg views `lake.gold.<relation>` | `lake-views` | The same public relations computed at read time from silver, over the full history, with natural keys instead of surrogate keys (decision [D39](decisions.md#d39--gold-in-the-lake-as-iceberg-views-over-silver)) |

The gold model, its rules and the business-question views are described in [the gold model](../lakehouse/gold/model.md), and every table and column in the [table reference](../reference/index.md), which the docs site renders from the Avro schemas.

## One micro-batch, end to end

Each hop is its own streaming query with its own checkpoint. A restart resumes from the checkpoint and neither skips nor repeats data. The table and file sinks are exactly once, and the gold merge is idempotent. The default trigger of every query is 10 seconds (`*_TRIGGER_SECONDS`).

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant L as landing-job
    participant S3 as SeaweedFS
    participant C as Iceberg catalog<br/>(analytics-db)
    participant B as bronze-job
    participant Si as silver-job
    participant G as gold-job
    participant PG as gold PostgreSQL

    L->>K: read the offsets after the checkpoint
    L->>S3: write Avro files, then commit them in _spark_metadata
    L->>S3: commit the batch in the landing checkpoint
    B->>S3: list the committed landing files
    B->>S3: write bronze and quarantine data files
    B->>C: commit an append snapshot per table
    Si->>C: read new bronze append snapshots
    Si->>S3: update the RocksDB deduplication state in the checkpoint
    Si->>C: commit an append snapshot per silver table
    G->>C: read new silver append snapshots
    G->>PG: replace the staging rows (Spark JDBC)
    G->>PG: BEGIN, writer lock, merge, re-resolve fact keys, COMMIT
    G->>S3: commit the batch in the gold checkpoint
    Note over G,PG: PostgreSQL commits before Spark records the batch.<br/>A batch replayed after a crash merges to the same rows.
```

## Operations

These jobs change the data outside the streaming path. Each one is safe to run while the streams run, unless the operation says otherwise.

```mermaid
flowchart TB
    landing[/"Landing files"/]
    bronze[("Bronze and quarantine")]
    silver[("Silver tables")]
    lake[("Bronze and silver<br/>Iceberg tables")]
    gold[("gold_internal")]

    replay["make bronze-replay<br/>TOPIC + offset or time range"]
    maint["maintenance-job / make lake-maintenance<br/>rewrite_data_files, expire_snapshots,<br/>remove_orphan_files"]
    guard{"Expiry guard:<br/>every downstream stream<br/>past the snapshots?"}
    rebuild["make gold-rebuild CONFIRM=1<br/>optional TO_TIME"]
    retention["gold-retention-job / make gold-retention<br/>apply_history_retention"]

    landing --> replay
    replay -- "appends only the changes<br/>bronze lacks" --> bronze
    maint -- "compaction: replace snapshots<br/>that streams skip" --> lake
    maint --> guard
    guard -- "yes: expire" --> lake
    guard -- "no: skip expiry, warn" --> lake
    silver --> rebuild
    rebuild -- "gold-reset, batch load up to TO_TIME<br/>through the gold sinks; gold job left stopped" --> gold
    retention -- "back-partition the default partition,<br/>drop months older than 36" --> gold

```

- **Bronze replay** (decision [D31](decisions.md#d31--late-arrivals-are-history-deduplication-happens-in-silver), #85): re-reads a landing range in batch and appends only the change identities bronze does not hold yet. Records that fail validation go to quarantine once. It writes only `append` snapshots, so silver keeps streaming.
- **Lake maintenance** (#26, #77): compaction writes `replace` snapshots, which Iceberg's streaming reader skips. Snapshot expiry keeps 7 days and the last 10 snapshots. Before expiring a table, the guard reads the last committed batch of every downstream stream from its checkpoint, and skips expiry when that snapshot would be lost. Orphan files older than 3 days are removed.
- **Gold rebuild** (decision [D38](decisions.md#d38--rebuild-gold-from-silver-with-an-optional-cutoff), #49): resets gold, reloads it from the silver tables in one batch up to an optional `_changed_at` cutoff, and applies retention. Restarting the gold job on fresh checkpoints then replays silver idempotently and continues past the cutoff.
- **Gold retention** (decision [D37](decisions.md#d37--gold-retention-by-monthly-partitions), #48): history facts keep 36 months (`GOLD_RETENTION_MONTHS`). Whole monthly partitions are dropped, after the rows in the default partition are moved into partitions of their own. Current state and dimensions are never trimmed, and the lake keeps the full history.

The data quality checks, which compare each layer with the one before it, are in [data quality checks](../lakehouse/data-quality.md#data-quality-chain).
