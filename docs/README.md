# Deel CDC pipeline

The design documentation of the pipeline that streams the supplied `finance_db` PostgreSQL database through Debezium and Kafka into an Iceberg lake (landing, bronze, silver) and a dimensional model in PostgreSQL (gold). The [repository README](https://github.com/jonathan5p/deel-home-task#readme) is the run guide.

- [Gold model](architecture/gold-model.md): the dimensional model in PostgreSQL, its rules, the business-question views and the indexes behind them.
- [Table reference](reference/index.md): every bronze, silver and gold table and view, with its columns, types and descriptions, rendered from the Avro schemas when the site is built.

Use the search box to find a table or a column.
