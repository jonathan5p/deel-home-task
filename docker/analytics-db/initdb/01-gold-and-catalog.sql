-- Runs only when the analytics-db data volume is empty; later edits need a fresh volume to apply.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE DATABASE iceberg_catalog;

\connect iceberg_catalog

-- The Iceberg JDBC catalog's own tables, as its schema version V1 defines them. Spark would create
-- them on first use, but the read-only REST catalog cannot, and fails to start without them.
CREATE TABLE iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255),
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

COMMENT ON TABLE iceberg_tables IS 'Every Iceberg table and view of the lake catalogs, with the location of its current metadata file.';
COMMENT ON COLUMN iceberg_tables.catalog_name IS 'The Iceberg catalog the table or view belongs to.';
COMMENT ON COLUMN iceberg_tables.table_namespace IS 'The namespace of the table or view, such as bronze or silver.';
COMMENT ON COLUMN iceberg_tables.table_name IS 'The name of the table or view in its namespace.';
COMMENT ON COLUMN iceberg_tables.metadata_location IS 'The current metadata file of the table or view in the lake.';
COMMENT ON COLUMN iceberg_tables.previous_metadata_location IS 'The metadata file the last commit replaced.';
COMMENT ON COLUMN iceberg_tables.iceberg_type IS 'TABLE or VIEW; empty for tables created before the catalog stored views.';

COMMENT ON TABLE iceberg_namespace_properties IS 'The properties of every namespace of the lake catalogs, one row per property.';
COMMENT ON COLUMN iceberg_namespace_properties.catalog_name IS 'The Iceberg catalog the namespace belongs to.';
COMMENT ON COLUMN iceberg_namespace_properties.namespace IS 'The namespace, such as bronze or silver.';
COMMENT ON COLUMN iceberg_namespace_properties.property_key IS 'The name of the property.';
COMMENT ON COLUMN iceberg_namespace_properties.property_value IS 'The value of the property.';
