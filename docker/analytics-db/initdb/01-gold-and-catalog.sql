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
