# Deel CDC pipeline

The design documentation of the pipeline that streams the supplied `finance_db` PostgreSQL database through Debezium and Kafka into an Iceberg lake (landing, bronze, silver) and a dimensional model in PostgreSQL (gold). The [repository README](https://github.com/jonathan5p/deel-home-task#readme) is the run guide: how to start the stack, run the pipeline and use each tool. These pages explain how the system is built and why.

## Architecture

- [Architecture overview](architecture/overview.md): every service, the data flow from the source database to gold, one micro-batch end to end, the replay, rebuild, retention and maintenance operations, and the data quality chain.
- [Spark sizing strategy](architecture/sizing.md): the variables the cluster is sized by, how cores and memory are shared between the jobs, the PostgreSQL query cost of the business views, the key streaming metrics, and scaling to production.
- [Design decisions](architecture/decisions.md): the main design decisions, the alternatives weighed, and links to their issues and pull requests.
- [Gold model](architecture/gold-model.md): the dimensional model in PostgreSQL, its rules, the business-question views and the indexes behind them.

## Reference

- [Table reference](reference/index.md): every [bronze](reference/bronze/index.md), [silver](reference/silver/index.md) and [gold](reference/gold/index.md) table and view, with its columns, types and descriptions. It is rendered from the Avro schemas each time the site is built (`make docs`), so it never needs editing by hand.

Use the search box to find a table or a column.
