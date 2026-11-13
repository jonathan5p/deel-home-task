# The way to production

This interview task runs on one machine, because the assignment asks for a stack that starts from the README ([D8](decisions.md#d8--fully-local-no-cloud-services)). The pipeline logic, the model and the checks are built as in production; the platform around them is simplified. This page lists what production needs instead. Planned next steps are issues in the [M8 · Ideas and experiments](https://github.com/jonathan5p/deel-home-task/milestone/7) milestone.

Two things make the move cheaper:

- **The code holds no deployment details:** brokers, topics, the lake, the catalog and the database are environment variables ([configuration](../operations/configuration.md)).
- **Every component speaks a standard protocol:** S3, Iceberg, Kafka with Debezium, and plain PostgreSQL.

## Summary

| Area | Here | In production | Tracked in |
| --- | --- | --- | --- |
| [Catalog](#catalog) | Iceberg JDBC catalog in the analytics PostgreSQL, plus Apache's reference REST server, read-only | A catalog service: Apache Polaris, AWS Glue Data Catalog, Unity Catalog or Project Nessie | |
| [Object store](#object-store) | One SeaweedFS container | AWS S3 or another managed S3-compatible store | |
| [Kafka](#kafka-and-the-source) | One broker, one partition per topic, JSON without a schema registry | Managed Kafka, replicated, with more partitions and a schema registry | #84 |
| [Security](#authentication-tls-and-secrets) | Local credentials in `.env`, plain-text connections, no API authentication | A secrets manager, TLS everywhere, IAM roles, authenticated API and dashboards | |
| [Gold serving](#gold-serving) | Gold in Iceberg, copied into one PostgreSQL; questions counted when read | A columnar serving store such as ClickHouse; incremental counts once the read-time scans grow | |
| [Spark](#spark-deployment-and-sizing) | Standalone cluster, client-mode driver containers, shared executors | YARN on EMR, EMR Serverless or Kubernetes, executors per application, sized from measured metrics | #58, #195 |
| [Scheduling](#orchestration-and-scheduling) | Compose services that loop with `sleep` | An orchestrator or scheduled cluster jobs, with retries and alerts | |
| [Monitoring](#monitoring-and-alerting) | Spark UI, History Server, a freshness view, Spark metrics in Prometheus and Grafana | Metrics in a time-series system, dashboards and alerts | #69, #126, #127, #194 |
| [Data quality](#data-quality) | Custom layer-to-layer checks | A data quality framework, with result history and alerting | #112, #116 |
| [Schema changes](#schema-evolution-and-migrations) | Explicit schemas, idempotent DDL, manual steps | Reviewed evolution rules and a migration tool | #196, #70 |
| [Testing and delivery](#testing-and-delivery) | Infra tests run locally only | Infra tests and deployments in CI/CD | |
| [Infrastructure](#infrastructure-as-code) | One Compose file | Infrastructure as code | |
| [Availability](#high-availability-and-backups) | One instance of everything, local volumes | Replicated services, backups and a recovery runbook | |

## Catalog

**Here.** Iceberg's JDBC catalog in the analytics PostgreSQL ([D9](decisions.md#d9--iceberg-with-a-jdbc-catalog-on-the-analytics-postgresql)), plus Apache's reference REST server, read-only, for other engines ([Iceberg REST catalog](infrastructure.md#iceberg-rest-catalog)).

**In production.** A catalog service that every engine shares, with access control, credential vending and an audit log: [Apache Polaris](https://polaris.apache.org/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html), [Unity Catalog](https://www.unitycatalog.io/) or [Project Nessie](https://projectnessie.org/). Spark then uses the REST catalog too, and the catalog no longer shares a database with gold.

## Object store

**Here.** One SeaweedFS container with static access keys ([D13](decisions.md#d13--object-store-seaweedfs-instead-of-minio)).

**In production.** AWS S3 or another managed S3-compatible store, by changing the endpoint and credentials. Add bucket encryption, versioning or object lock on the landing files, and lifecycle rules for old reports and event logs.

## Kafka and the source

**Here.** One Kafka broker with ZooKeeper, one partition per topic, and JSON messages without a schema registry. The source database and the Debezium connector are used as supplied ([D29](decisions.md#d29--the-source-database-and-the-debezium-connector-stay-as-provided)).

**In production.**

- Managed Kafka, such as Amazon MSK or Confluent Cloud, with a replication factor of three and enough partitions per topic for the read parallelism the sizing needs.
- A schema registry, with Avro or Protobuf messages and compatibility rules, so a source change is checked before it reaches the pipeline. Landing keeps the raw bytes, so it stays valid when the format changes ([D18](decisions.md#d18--landing-as-raw-avro-files-one-folder-per-topic)).
- Debezium on a managed Kafka Connect cluster, with the source-side recommendations applied: `decimal.handling.mode=string` and a publication limited to the four tables (#84).
- For the data generator: attach each new order's items to the order it just inserted (`INSERT ... RETURNING order_id`), so open orders get items and "pending items by product" shows data ([why it is empty today](../lakehouse/gold/model.md#things-to-know-about-the-source-data)).

## Authentication, TLS and secrets

**Here.** Local credentials in `.env`, plain-text connections, no API authentication, anonymous Grafana viewers. Consumers already use [read-only service logins](infrastructure.md#read-only-service-logins).

**In production.**

- Secrets in a secrets manager (AWS Secrets Manager, HashiCorp Vault), injected at run time and rotated.
- IAM roles for S3 and the catalog instead of static keys.
- TLS on Kafka, PostgreSQL, the object store and every HTTP endpoint.
- Authentication on the API (OAuth or an API gateway) and single sign-on for Grafana.

## Gold serving

**Here.** Spark computes gold in four streams, stores it in Iceberg and copies each batch into PostgreSQL ([D44](decisions.md#d44--gold-modelled-in-spark-on-iceberg-postgresql-as-serving-copy)). PostgreSQL serves the business questions, counted from the open orders when read, and keeps 36 months of history; the lake keeps all of it.

**In production.**

- **A columnar serving store** such as ClickHouse, once the facts or readers outgrow one PostgreSQL ([D3](decisions.md#d3--gold-in-postgresql-not-clickhouse)). The logic stays in Spark, so only the copy step changes; readers still need each stream's batch to appear whole.
- **PostgreSQL read replicas** meanwhile, for more readers.
- **Incremental counts with retractions,** once scanning the open orders on every read grows too large: each changed order subtracts one from its old bucket and adds one to its new one, as Flink, Kafka Streams or Materialize do. It needs the previous version of each order, an exactly-once marker written with the counters, and a periodic full recount. In Spark: `foreachBatch` deltas or `transformWithState`.
- **Storage-partitioned joins** for the merges, once verified on the versions in use.

## Spark deployment and sizing

**Here.** A Spark standalone cluster with two workers. Every application gets a throwaway driver container in client mode, and all applications share the executors, each capped with `spark.cores.max` ([D27](decisions.md#d27--one-driver-container-per-application-executors-on-shared-workers)).

**In production.**

- **Driver placement:** an EMR step in YARN cluster mode (EMR Serverless for batch jobs), or the Spark Operator on Kubernetes.
- **Executors per application,** sized from measured metrics, with dynamic allocation for batch jobs ([scaling to production](sizing.md#scaling-to-production), #58, #195).
- **Event logs on the object store,** with the History Server's cleaner.

## Orchestration and scheduling

**Here.** Maintenance, retention and quality checks are Compose services that run, sleep and repeat; Docker restarts the streams ([maintenance and recovery](../operations/maintenance-and-recovery.md)).

**In production.** Scheduled cluster jobs or an orchestrator (Airflow, Dagster, Step Functions), with retries, backfills, run history and alerts. The bronze replay and gold rebuild become parameterised jobs instead of `make` targets.

## Monitoring and alerting

**Here.** The Spark UI and History Server, the freshness view and endpoint, Spark metrics in Prometheus and Grafana with two alert rules that notify no one ([Monitoring Spark](../operations/monitoring.md)), and a latency probe ([end-to-end latency](../operations/latency.md)).

**In production.**

- Spark, Kafka and PostgreSQL metrics in a time-series system, with dashboards (#126).
- Alerts on what breaks the promise to the users: a batch duration above its trigger, a growing consumer lag, freshness above the target, a failed data quality check, a stopped stream.
- Per-batch pipeline statistics in an observability catalog, apart from business data (#127, [D33](decisions.md#d33--pipeline-metrics-in-a-separate-observability-catalog-proposed)).

## Data quality

**Here.** My own layer-to-layer checks, from Kafka to gold, with JSON reports in the lake ([data quality checks](../lakehouse/data-quality.md)).

**In production.** A framework such as [Deequ](https://github.com/awslabs/deequ), [Great Expectations](https://greatexpectations.io/) or [Soda](https://www.soda.io/), for declarative checks, profiling, result history and alerting (#112, #116). The layer reconciliation stays, written in the framework.

## Schema evolution and migrations

**Here.** Explicit Avro schemas, so a source change never alters the lake silently ([bronze](../lakehouse/bronze.md#schema-evolution)). The gold DDL is idempotent, with no migration tool.

**In production.**

- A reviewed schema evolution policy across the lake and the analytics database: which changes flow through automatically, which need a migration, and how consumers are told (#196).
- A schema registry in front of the pipeline, as above.
- A migration tool for the analytics database, such as Flyway, Liquibase or dbmate (#70).

## Testing and delivery

**Here.** CI runs lint, type checks and the unit tests on every pull request, and builds and deploys this site. The infra tests start full copies of the stack and run locally only ([testing and CI](../development/testing.md)).

**In production.** Infra tests in CI too, on large runners, per platform change or nightly. A delivery pipeline builds and versions the images, deploys to staging, runs the data quality checks and promotes.

## Infrastructure as code

**Here.** One `docker-compose.yaml` with profiles, driven by `make`.

**In production.** Terraform, OpenTofu or the AWS CDK for the cloud resources, and Helm charts or operators on Kubernetes, so every environment comes from the same code.

## High availability and backups

**Here.** One instance of every service, with data in local Docker volumes.

**In production.**

- PostgreSQL as a managed, replicated service (Amazon RDS or Aurora, Multi-AZ), with point-in-time recovery.
- Kafka replicated across availability zones, with retention long enough to recover from a stopped stream.
- The lake on S3, with versioning on the landing files.
- A tested recovery runbook. The building blocks exist: [replaying a landing range into bronze](../operations/maintenance-and-recovery.md#replaying-a-landing-range-into-bronze), [rebuilding gold from silver](../operations/maintenance-and-recovery.md#rebuilding-gold-from-silver) and [rebuilding bronze from landing](../operations/maintenance-and-recovery.md#rebuilding-bronze-from-landing).

## Further ideas

The [M8 · Ideas and experiments](https://github.com/jonathan5p/deel-home-task/milestone/7) milestone collects the experiments that would come next, such as a fast path from bronze to gold if latency matters (#87), and a columnar serving store for gold if the facts outgrow PostgreSQL ([Gold serving](#gold-serving)).
