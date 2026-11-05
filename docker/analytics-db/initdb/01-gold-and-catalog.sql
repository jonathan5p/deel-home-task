-- Runs only when the analytics-db data volume is empty; later edits need a fresh volume to apply.

CREATE SCHEMA IF NOT EXISTS gold;

-- The Iceberg JDBC catalog creates its own tables in this database on first use.
CREATE DATABASE iceberg_catalog;
