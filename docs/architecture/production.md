# The way to production

This project is an interview task that runs on one machine, because the assignment asks for a stack reviewers can start from the README ([decision D8](decisions.md#d8--fully-local-no-cloud-services)). The pipeline logic, the data model and the checks are built as they would be in production; the platform around them is simplified. This page lists what is left to take it to a real production environment: what was left out or simplified, and what production needs instead.

The planned next steps are tracked as issues in the [M8 · Ideas and experiments](https://github.com/jonathan5p/deel-home-task/milestone/7) milestone; each area below links its issues where one exists.

Two things make the move cheaper than it looks:

- **The code holds no deployment details.** Brokers, topics, the lake root, the catalog and the database are required environment variables, so another environment is a new set of values, not a code change ([configuration](../operations/configuration.md)).
- **Every component speaks a standard protocol.** The lake speaks S3, the tables are standard Iceberg (gold included), the source is Kafka with Debezium, and gold is served from plain PostgreSQL.

## Summary

| Area | Here | In production | Tracked in |
| --- | --- | --- | --- |
| [Catalog](#catalog) | Iceberg JDBC catalog in the analytics PostgreSQL, plus Apache's reference REST server, read-only | A catalog service: Apache Polaris, AWS Glue Data Catalog, Unity Catalog or Project Nessie | |
| [Object store](#object-store) | One SeaweedFS container | AWS S3 or another managed S3-compatible store | |
| [Kafka](#kafka-and-the-source) | One broker, one partition per topic, JSON without a schema registry | Managed Kafka, replicated, with more partitions and a schema registry | #84 |
| [Security](#authentication-tls-and-secrets) | Local credentials in `.env`, plain-text connections, no API authentication | A secrets manager, TLS everywhere, IAM roles, authenticated API and dashboards | |
| [Gold serving](#gold-serving) | Spark writes gold to Iceberg and copies it into one PostgreSQL that serves the API and Grafana; the business questions are views counted when read | A columnar serving store, such as ClickHouse, fed from the same batches; incremental counts with retractions once the read-time scans grow | |
| [Spark](#spark-deployment-and-sizing) | Standalone cluster, client-mode driver containers, shared executors | YARN on EMR, EMR Serverless or Kubernetes, executors per application, sized from measured metrics | #58, #195 |
| [Scheduling](#orchestration-and-scheduling) | Compose services that loop with `sleep` | An orchestrator or scheduled cluster jobs, with retries and alerts | |
| [Monitoring](#monitoring-and-alerting) | Spark UI, History Server, a freshness view, Spark metrics in Prometheus and Grafana | Metrics in a time-series system, dashboards and alerts | #69, #126, #127, #194 |
| [Data quality](#data-quality) | Custom layer-to-layer checks | A data quality framework, with result history and alerting | #112, #116 |
| [Schema changes](#schema-evolution-and-migrations) | Explicit schemas, idempotent DDL, manual steps | Reviewed evolution rules and a migration tool | #196, #70 |
| [Testing and delivery](#testing-and-delivery) | Infra tests run locally only | Infra tests and deployments in CI/CD | |
| [Infrastructure](#infrastructure-as-code) | One Compose file | Infrastructure as code | |
| [Availability](#high-availability-and-backups) | One instance of everything, local volumes | Replicated services, backups and a recovery runbook | |

## Catalog

**Here.** Spark uses Iceberg's JDBC catalog, stored in the analytics PostgreSQL ([D9](decisions.md#d9--iceberg-with-a-jdbc-catalog-on-the-analytics-postgresql)). For other engines, the `catalog` profile serves it through `apache/iceberg-rest-fixture`, Apache's reference implementation of the REST catalog, with a read-only login ([Iceberg REST catalog](infrastructure.md#iceberg-rest-catalog)).

**In production.** A catalog service that every engine shares, with access control, credential vending and an audit log: [Apache Polaris](https://polaris.apache.org/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html), [Unity Catalog](https://www.unitycatalog.io/) or [Project Nessie](https://projectnessie.org/). Spark then uses the REST catalog too, and the catalog no longer shares a database with gold.

## Object store

**Here.** One SeaweedFS container with static access keys ([D13](decisions.md#d13--object-store-seaweedfs-instead-of-minio)).

**In production.** AWS S3, or another managed S3-compatible store: a configuration change of the endpoint and the credentials. Add bucket encryption, versioning or object lock on the landing files, and lifecycle rules for old quality reports and event logs. Checkpoints already live in the object store, so they move with it.

## Kafka and the source

**Here.** One Kafka broker with ZooKeeper, one partition per topic, and JSON messages without a schema registry. The source database and the Debezium connector are used as supplied ([D29](decisions.md#d29--the-source-database-and-the-debezium-connector-stay-as-provided)).

**In production.**

- Managed Kafka, such as Amazon MSK or Confluent Cloud, with a replication factor of three and enough partitions per topic for the read parallelism the sizing needs.
- A schema registry, with Avro or Protobuf messages and compatibility rules, so a source change is checked before it reaches the pipeline. Landing keeps the raw bytes, so it stays valid when the format changes ([D18](decisions.md#d18--landing-as-raw-avro-files-one-folder-per-topic)).
- Debezium on a managed Kafka Connect cluster, with the source-side recommendations applied: `decimal.handling.mode=string` and a publication limited to the four tables (#84).
- A recommendation for the data generator: attaching each new order's items to the order it has just inserted (`INSERT ... RETURNING order_id`), rather than looking the order up by customer and date, would give open orders their items, so "pending items by product" would show data. The supplied generator is kept as it is; [why there are no pending items today](../lakehouse/gold/model.md#things-to-know-about-the-source-data).

## Authentication, TLS and secrets

**Here.** Every credential is a local development default in `.env`, marked as such, and every connection is plain text. The API has no authentication, and Grafana lets anonymous visitors view. Consumers already log in with read-only service logins that can read only the `gold` views ([read-only service logins](infrastructure.md#read-only-service-logins)).

**In production.**

- Secrets in a secrets manager (AWS Secrets Manager, HashiCorp Vault), injected at run time and rotated.
- IAM roles for S3 and the catalog instead of static keys.
- TLS on Kafka, PostgreSQL, the object store and every HTTP endpoint.
- Authentication on the API (OAuth or an API gateway) and single sign-on for Grafana.

## Gold serving

**Here.** The gold job computes the model with Spark in four independent streams, stores it as Iceberg tables in `lake.gold_internal`, and copies each micro-batch's computed rows into PostgreSQL with plain upserts ([D44](decisions.md#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy)). Both stores render the same views; PostgreSQL serves the business questions, counted when read from the open orders through partial indexes, and keeps 36 months of history; the lake keeps all of it for analytical engines. Lake maintenance already compacts the gold tables, position delete files included.

**In production.**

- **A columnar serving store for scale.** When the facts or the number of readers outgrow one PostgreSQL, a store built for analytical serving, such as ClickHouse, takes the copy's place ([D3](decisions.md#d3--gold-in-postgresql-not-clickhouse)). The model's logic stays in Spark, so only the copy step changes: it writes the same computed rows, and the snapshot bookkeeping in `sync_state` carries over. ClickHouse's eventual deduplication matters less when every row arrives already computed, but readers still need each stream's batch to appear whole, as PostgreSQL's transaction gives today.
- **PostgreSQL read replicas** meanwhile, for more API and Grafana readers.
- **Incremental counts for the business questions.** The views scan the open orders on every read, which is exact and cheap while they are a few thousand rows. When that scan grows too large, keep the counts incrementally with retractions, as Flink changelog streams, Kafka Streams table aggregations (an adder and a subtractor) or Materialize do: every changed order subtracts one from its old bucket (delivery date, status, customer) and adds one to its new one. That needs the previous version of each order, the same newer-only rule as current state, an exactly-once marker written in the same transaction as the counters so a replayed batch does not count twice, and a periodic full recount to correct any drift. In Spark it fits `foreachBatch` deltas or `transformWithState`. The top 3 can then be ranked over a candidate set, the current top N plus the keys the batch touched, with a periodic full re-rank.
- **Storage-partitioned joins for the merges,** once verified on the Spark and Iceberg versions in use, so a merge into a large history table shuffles less.

## Spark deployment and sizing

**Here.** A Spark standalone cluster with two workers. Every application gets a throwaway driver container in client mode, and all applications share the executors, each capped with `spark.cores.max` ([D27](decisions.md#d27--one-driver-container-per-application-executors-on-shared-workers)).

**In production.**

- **Driver placement.** On Amazon EMR, each streaming job is a step in YARN cluster mode, so the driver runs in the cluster with YARN's restart attempts; EMR Serverless suits the batch jobs. On Kubernetes, the Spark Operator gives each application its own driver and executor pods with a restart policy.
- **Executors per application**, sized from the batch size and the state each job holds, with dynamic allocation for the batch jobs.
- **Sizing from measured metrics.** The [sizing strategy](sizing.md) names the variables and metrics; the cluster and the stream partitions should be sized from a real run's numbers (#58, #195). [Scaling to production](sizing.md#scaling-to-production) covers parallelism, rates and memory.
- **Event logs on the object store**, with the History Server's cleaner removing old logs.

## Orchestration and scheduling

**Here.** Lake maintenance, gold retention and the data quality checks run as optional Compose services that run their job, sleep for a set number of hours and start again; a failed run waits for the next one ([maintenance and recovery](../operations/maintenance-and-recovery.md)). The streaming jobs are Compose services restarted by Docker.

**In production.** The periodic jobs become scheduled jobs on the cluster (a scheduled Databricks job, an EMR step) or tasks in an orchestrator (Apache Airflow, Dagster, AWS Step Functions with EventBridge) that submits them. The orchestrator adds retries, backfills, dependencies between jobs, run history and alerts on failure. The recovery operations, the bronze replay and the gold rebuild, become parameterised jobs there instead of `make` targets.

## Monitoring and alerting

**Here.** Each job's Spark UI, the History Server for past runs, the `gold.data_freshness` view and the API's `/freshness` endpoint. With the `metrics` profile, Prometheus scrapes Spark's own metrics, Grafana charts them and two alert rules fire without notifying anyone ([Monitoring Spark](../operations/monitoring.md)). A probe measures the end-to-end latency per entity ([End-to-end latency](../operations/latency.md)).

**In production.**

- Spark, Kafka and PostgreSQL metrics in a time-series system, with dashboards (#126).
- Alerts on what breaks the promise to the users: a batch duration above its trigger, a growing consumer lag, freshness above the target, a failed data quality check, a stopped stream.
- Per-batch pipeline statistics in an observability catalog, apart from business data (#127, [D33](decisions.md#d33--pipeline-metrics-in-a-separate-observability-catalog-proposed)).

## Data quality

**Here.** The project builds its own checks: each one compares a layer with the one before it, from Kafka to gold, and writes a JSON report to the lake ([data quality checks](../lakehouse/data-quality.md)). They keep the stack small and show the reconciliation logic.

**In production.** A data quality framework instead: [Deequ](https://github.com/awslabs/deequ) or [PyDeequ](https://github.com/awslabs/python-deequ) on Spark, [Great Expectations](https://greatexpectations.io/) or [Soda](https://www.soda.io/). They add declarative checks, profiling, anomaly detection, a history of results and alerting (#112, #116). The reconciliation between layers stays, as checks written in the framework.

## Schema evolution and migrations

**Here.** Every table is built from explicit Avro schemas, so a source change never alters the lake silently; bronze documents which source changes are automatic and which need a manual step ([bronze](../lakehouse/bronze.md)). The gold DDL is idempotent SQL applied on start, without a migration tool.

**In production.**

- A reviewed schema evolution policy across the lake and the analytics database: which changes flow through automatically, which need a migration, and how consumers are told (#196).
- A schema registry in front of the pipeline, as above.
- A migration tool for the analytics database, such as Flyway, Liquibase or dbmate (#70).

## Testing and delivery

**Here.** CI runs lint, type checks and the unit tests on every pull request, and builds and deploys this site. The infra tests start full copies of the stack and run locally only ([testing and CI](../development/testing.md)).

**In production.** The infra tests also run in CI, on runners with enough memory for the stack, on every pull request that touches the platform or nightly. A delivery pipeline builds and versions the images, deploys them to a staging environment, runs the data quality checks there, and promotes them.

## Infrastructure as code

**Here.** One `docker-compose.yaml` with profiles, driven by `make`.

**In production.** The cloud resources (Kafka, the cluster, buckets, databases, IAM, networking) described as code, with Terraform, OpenTofu or the AWS CDK, and the services on Kubernetes as Helm charts or operators. Every environment is created from the same code.

## High availability and backups

**Here.** One instance of every service, with data in local Docker volumes.

**In production.**

- PostgreSQL as a managed, replicated service (Amazon RDS or Aurora, Multi-AZ), with point-in-time recovery.
- Kafka replicated across availability zones, with retention long enough to recover from a stopped stream.
- The lake on S3, with versioning on the landing files.
- A tested recovery runbook. The building blocks exist: [replaying a landing range into bronze](../operations/maintenance-and-recovery.md#replaying-a-landing-range-into-bronze), [rebuilding gold from silver](../operations/maintenance-and-recovery.md#rebuilding-gold-from-silver) and [rebuilding bronze from landing](../operations/maintenance-and-recovery.md#rebuilding-bronze-from-landing).

## Further ideas

The [M8 · Ideas and experiments](https://github.com/jonathan5p/deel-home-task/milestone/7) milestone collects the experiments that would come next, such as a fast path from bronze to gold if latency matters (#87), and a columnar serving store for gold if the facts outgrow PostgreSQL ([Gold serving](#gold-serving)).
