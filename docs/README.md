# Deel CDC pipeline

The documentation of the pipeline that streams the supplied `finance_db` PostgreSQL database through Debezium and Kafka into an Iceberg lake (landing, bronze, silver) and a dimensional model in PostgreSQL (gold). The [repository README](https://github.com/jonathan5p/deel-home-task#readme) is the quick start: how to install the stack, reproduce the results and debug it. These pages cover each part of the project in depth, how it is built and why.

## Layers

- [Source database and CDC](lakehouse/source-and-cdc.md): the supplied database, the Debezium topics and the change event format.
- [Landing](lakehouse/landing.md): the raw Avro copy of every Kafka record, and how to inspect it with Spark SQL.
- [Bronze](lakehouse/bronze.md): the typed change log, its quarantine, the content hash and schema evolution.
- [Silver](lakehouse/silver.md): the deduplicated version history and its history and current views.
- [Gold](lakehouse/gold/writer.md): the dimensional model in PostgreSQL, the gold job and the gold views in the lake.

## Operations

- [Running the pipeline](operations/running-the-pipeline.md): the streaming jobs as Compose services, their UIs and cores.
- [Data quality checks](lakehouse/data-quality.md): the chain of checks from Kafka to gold, and their reports.
- [Maintenance and recovery](operations/maintenance-and-recovery.md): lake maintenance, gold retention, the gold rebuild, the bronze replay and rebuild.
- [Debugging](operations/debugging.md): where to look, common errors, resets and the optional Kafka console.

## Serving

- [Analytics API](serving/api.md): the business questions and freshness over HTTP.
- [Grafana](serving/grafana.md): the gold metrics dashboard, provisioned as code.

## Platform

- [Services and storage](architecture/infrastructure.md): the Compose stack, the analytics database and its logins, and the SeaweedFS lake.
- [Spark cluster](platform/spark.md): the cluster, the Iceberg catalog, the bundled connectors, submitting jobs and the History Server.
- [Configuration](operations/configuration.md): every environment variable the code reads.

## Development

- [Code layout](development/code-layout.md): the uv workspace and its projects.
- [Table and column definitions](development/table-definitions.md): the Avro schemas every table and column is named and described from.
- [Testing and CI](development/testing.md): unit tests and coverage, infra tests, hooks and continuous integration.

## Architecture

- [Architecture overview](architecture/overview.md): every service, the data flow from the source database to gold, one micro-batch end to end, the replay, rebuild, retention and maintenance operations, and the data quality chain.
- [Spark sizing strategy](architecture/sizing.md): the variables the cluster is sized by, how cores and memory are shared between the jobs, the PostgreSQL query cost of the business views, the key streaming metrics, and scaling to production.
- [Design decisions](architecture/decisions.md): the main design decisions, the alternatives weighed, and links to their issues and pull requests.
- [Gold model](lakehouse/gold/model.md): the dimensional model in PostgreSQL, its rules, the business-question views and the indexes behind them.

## Reference

- [Table reference](reference/index.md): every [bronze](reference/bronze/index.md), [silver](reference/silver/index.md) and [gold](reference/gold/index.md) table and view, with its columns, types and descriptions. It is rendered from the Avro schemas each time the site is built (`make docs`), so it never needs editing by hand.
- [Supplied assignment README](reference/original-readme.md): the README delivered with the take-home, as it was.

Use the search box to find a table or a column.
