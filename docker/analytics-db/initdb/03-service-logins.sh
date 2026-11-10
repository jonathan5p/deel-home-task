#!/bin/sh
set -eu

create_login() {
    psql --no-psqlrc --quiet -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -v login="$1" -v password="$2" -v statement_timeout="$3" <<'SQL'
SELECT format('CREATE ROLE %I', :'login')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'login') \gexec
SELECT format(
    'ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L',
    :'login', :'password'
) \gexec
SELECT format('GRANT gold_reader TO %I', :'login') \gexec
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'login') \gexec
SELECT format('ALTER ROLE %I SET statement_timeout = %L', :'login', :'statement_timeout') \gexec
SQL
}

create_login "$API_READER_USER" "$API_READER_PASSWORD" 5s
