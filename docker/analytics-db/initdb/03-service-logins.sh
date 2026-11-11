#!/bin/sh
set -eu

run_sql() {
    psql --no-psqlrc --quiet -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$@"
}

read_only_login() {
    run_sql "$POSTGRES_DB" -v login="$1" -v password="$2" <<'SQL'
SELECT format('CREATE ROLE %I', :'login')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'login') \gexec
SELECT format(
    'ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L',
    :'login', :'password'
) \gexec
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'login') \gexec
SQL
}

create_login() {
    read_only_login "$1" "$2"
    run_sql "$POSTGRES_DB" -v login="$1" -v statement_timeout="$3" <<'SQL'
SELECT format('GRANT gold_reader TO %I', :'login') \gexec
SELECT format('ALTER ROLE %I SET statement_timeout = %L', :'login', :'statement_timeout') \gexec
SQL
}

create_catalog_login() {
    read_only_login "$1" "$2"
    run_sql iceberg_catalog -v login="$1" <<'SQL'
SELECT format('GRANT CONNECT ON DATABASE iceberg_catalog TO %I', :'login') \gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', :'login') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I', :'login') \gexec
SQL
}

create_login "$API_READER_USER" "$API_READER_PASSWORD" 5s
create_login "$GRAFANA_READER_USER" "$GRAFANA_READER_PASSWORD" 30s
create_catalog_login "$LAKE_CATALOG_READER_USER" "$LAKE_CATALOG_READER_PASSWORD"
