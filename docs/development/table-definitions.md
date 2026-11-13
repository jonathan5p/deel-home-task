# Table and column definitions

The [docs site](https://jonathan5p.github.io/deel-home-task/) has a table reference with every bronze, silver and gold table and column, its type, nullability and description. It is rendered from these schemas each time the site is built: `make docs` builds it into the git-ignored `target/site/`, `make docs-serve` serves it locally, and the `docs` workflow publishes it to GitHub Pages from `main`.

The pipelines name and describe their tables and columns from one set of [Avro schemas](https://avro.apache.org/docs/1.12.0/specification/) (`.avsc`, JSON) in `analytics/pipelines/src/pipelines/schemas/`, shipped as package data of `pipelines` (in the wheel and in the executors' `packages.zip`):

```text
audit.avsc                        shared record: the audit columns every source table ends with (updated_at, updated_by, created_at, created_by)
sources/<table>.avsc              one source table of the operations schema (customers, products, orders, order_items)
tables/<table>_template.avsc      one table or view a layer keeps per entity (bronze, bronze_quarantine, silver, silver_history,
                                  silver_current): its description, the column groups it includes and the full definition of the
                                  layer's own columns
gold/<table>.avsc                 one gold table, in Iceberg and in PostgreSQL (dim_customer, ..., current_order_items), or a view without a table
                                  (data_freshness and the four business question views)
definitions.py                    the loader
gold_definitions.py               the loader of the gold schemas
render.py                         renders every resulting table and view as a flat Avro schema (make schemas, into target/schemas/)
landing.py                        the Spark schema of the landed Kafka records (KAFKA_RECORD_SCHEMA)
```

A source table is one Avro record: `name` is the table, `namespace` the source schema, `doc` the table description, and each field carries its column's `name`, `type` and `doc` (its description), in the source's column order:

```json
{
  "type": "record",
  "name": "order_items",
  "namespace": "operations",
  "doc": "Line items of orders: a product and the quantity ordered",
  "key": "order_item_id",
  "include": ["audit"],
  "fields": [
    {
      "name": "order_item_id",
      "type": "long",
      "doc": "Order item identifier, the source table's primary key"
    },
    {
      "name": "quanity",
      "type": ["null", "int"],
      "default": null,
      "doc": "Number of units ordered (source column quanity)",
      "renamed_to": "quantity"
    }
  ]
}
```

A table template is the layout of one table a layer keeps per entity. It is an ordinary Avro record whose `fields` fully define the columns the layer adds itself, and whose `include` lists the column groups that come before them: `source` is the entity's own fields (under their lake names) and `audit` the shared fields of `audit.avsc` (for the entities that include them). Its `doc` is the table description, with `{entity}` replaced by the source table's name and `{entity_description}` by its description. `partitioned_by` names one of its own timestamp or date fields; the table is partitioned by that field's day:

```json
{
  "type": "record",
  "name": "bronze_template",
  "namespace": "lake",
  "doc": "{entity_description}. Change log of source table {entity}: one typed row per Debezium change event ...",
  "include": ["source", "audit"],
  "partitioned_by": "_published_at",
  "fields": [
    {
      "name": "_record_source",
      "type": "string",
      "doc": "Kafka topic the change event was read from"
    },
    ...
    {
      "name": "_content_hash",
      "type": "string",
      "doc": "Hex SHA-256 of the content columns (key and audit columns excluded); unchanged content keeps its hash"
    }
  ]
}
```

So `lake.bronze.order_items` is the source fields of `order_items`, then its audit fields, then the nine fields of `bronze_template.avsc`; the quarantine template includes nothing (`"include": []`) and its table is just its own fields.

- **Why this shape:** every description is defined once, next to the table that owns the column, and each template reads as the table it produces. Avro can reference a named record from another file, but a reference nests the fields under one column instead of flattening them, so the flat composition (`include` on source records and templates) is our own convention, done by the loader.
- **The resulting tables, flat:** `make schemas` renders the full flat Avro schema of every lake table and view (each template plus the entity's source and audit columns, with every column's type, nullability and description) into the git-ignored `target/schemas/<layer>/<table>.avsc` (`target/schemas/bronze/orders.avsc`, `orders_quarantine.avsc`, `target/schemas/silver/orders_history.avsc`, `orders_current.avsc` and so on), as plain, standard Avro records with namespace `lake.<layer>` and the filled-in table description. They are for reading only: like dbt's `target/`, they are build output, regenerated on every run and never committed. To change a table, edit the definitions in `analytics/pipelines/src/pipelines/schemas/`, not the rendered output. A unit test checks the rendered columns and types against the schema the bronze and silver jobs actually write.
- **Types** follow Avro: `long` (BIGINT), `int` (INTEGER), `string` (VARCHAR), `boolean`, `{"type": "long", "logicalType": "timestamp-millis"}` (TIMESTAMP(3)), `{"type": "int", "logicalType": "date"}` and `{"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 10}`. The source DECIMAL declares no precision, so it has no exact Avro equivalent: the schemas declare DECIMAL(38,10), the type bronze stores. The lake layers map these types to Spark types in `pipelines/lake/shared/types.py`.
- **Nullability** follows Avro too: a column that is NOT NULL in the source, or always set by the pipeline, has the plain type; a nullable one is the union `["null", <type>]` with `"default": null`.
- **Custom properties** carry what Avro has no attribute for (Avro allows extra attributes and ignores them): `key` on a source record names its primary key, `renamed_to` on a field gives its name in the lake when the source name is misspelled, and `include` composes column groups as above.
- **A metadata (`_`) column used by several schemas** (such as the Kafka coordinates, in bronze and its quarantine, or `_commit_sequence` in bronze and gold) is defined in each, and a unit test checks that every definition of it, in any `.avsc` under `pipelines/schemas/`, has the same type, nullability and description. Business columns are exempt: a key and the references to it share a name but not a description or nullability.
- **Gold columns point at the column they come from** instead of repeating its description. A gold table is one record in `gold/` whose fields list every column in table order with its type and nullability in gold; a field has either its own `doc`, for what only gold has (surrogate keys, `line_amount`), or `"from"`, which takes the description of `<source table>.<lake name>` or `gold.<table>.<column>` (a gold field with its own `doc`). The type must match the referenced column's. Metadata columns (`_commit_sequence`, `_changed_at`, `_valid_to`, ...) are defined in full in each gold record, like in the layer templates, and the consistency test keeps every definition of a `_` column the same across all schemas. `doc` describes the table in `gold_internal`, and an optional `public_doc` its view in `gold` when that differs.

`pipelines.schemas.definitions` reads the files with the standard `json` module, so the Spark image needs no Avro library, into frozen dataclasses: `load_entities()` and `load_entity(name)` (`EntityDefinition`: `name`, `key`, `description`, `columns`, `included_columns`, `all_columns`), `load_shared_columns(name)` and `load_table_templates()` (`TableTemplate`, by name without `_template`: `name`, `description`, `include` (`ColumnGroup`s), `fields`, `layer`, `describe(entity)`, `table_name(entity)`, `columns_for(entity)`). A column is a `ColumnDefinition` (`name`, `type`, `description`, `not_null`, `renamed_to`, `lake_name`). The loader refuses, naming the file, a type it does not support, an unknown property, a missing or blank `doc`, a nullable field without `"default": null`, a key that is not a field, an unknown include, a template without columns or whose name does not end in `_template`, a record whose `name` differs from its file name, or two columns with the same lake name. A unit test also checks every file, and every source table with its included fields, with `fastavro.parse_schema` (a development dependency only).

To add or change a column or a description, edit the schema file (a new field goes where the column is in the source; a new pipeline column goes in the `fields` of each template that holds it, defined the same way in each), then restart the job (run `make schemas` to read the resulting tables): the descriptions of existing tables are updated on start and changed views are replaced, while new table columns also need `ALTER TABLE ... ADD COLUMN` (see schema evolution in the bronze layer). Column names used in code stay typed (the `MetadataColumn` enum in `pipelines/lake/shared/columns.py`, shared by every layer, and bronze's `RawColumn`), and tests check that each of them is defined in a template and that the fields of each bronze and silver template are exactly the columns the job writes to that table or the view returns.
