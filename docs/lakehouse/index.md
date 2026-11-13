# Lakehouse

The lakehouse follows a medallion architecture: bronze, silver and gold, each layer refining the one before it. In front of it, the source database with its CDC stream and the landing zone are the ingestion: Debezium captures every change of the source into Kafka, and landing keeps a raw, replayable copy of every Kafka record.

```mermaid
flowchart LR
    source[("Source<br/>finance_db, Debezium")] --> kafka[["Kafka"]]
    kafka --> landing[/"Landing<br/>raw Avro"/]
    landing --> bronze[("Bronze<br/>typed change log")]
    bronze --> silver[("Silver<br/>version history")]
    silver --> gold[("Gold<br/>dimensional model, Iceberg<br/>4 independent streams")]
    gold --> serving[("PostgreSQL<br/>serving copy, same views")]
```

Each layer has its own page: its purpose, what it reads and writes, its model, the job that writes it and what it guarantees.

- [Source database and CDC](source-and-cdc.md): the supplied database, the Debezium topics and the change event format.
- [Landing](landing.md): the raw Avro copy of every Kafka record, and how to inspect it with Spark SQL.
- [Bronze](bronze.md): the typed change log, its quarantine, the content hash and schema evolution.
- [Silver](silver.md): the deduplicated version history and its history and current views.
- [Gold](gold/index.md): the dimensional model, computed by Spark into Iceberg tables in the lake and copied into PostgreSQL for serving, with the same views in both; its [model](gold/model.md) and its [writer](gold/writer.md).
- [Data quality checks](data-quality.md): the checks run between the layer boundaries, each one comparing a layer with the one before it, from Kafka to gold.

Where each layer is stored and what it guarantees, side by side, is in the [architecture overview](../architecture/overview.md#data-flow); every table and column is in the [table reference](../reference/index.md).
