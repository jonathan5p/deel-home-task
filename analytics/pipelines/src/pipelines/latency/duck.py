from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote

import duckdb

from pipelines.latency.settings import LakeReadSettings, LatencySettings
from pipelines.latency.sql import Row, duckdb_path, identifier, literal

EXTENSIONS = ("httpfs", "avro", "iceberg", "postgres")
REST_WAREHOUSE = "warehouse"
SOURCE_ALIAS = "source_db"
GOLD_ALIAS = "analytics_db"
SINK_LOG_FOLDER = "_spark_metadata"

type DuckDbConfig = dict[str, str | bool | int | float | list[str]]


class MissingExtensionError(RuntimeError):
    def __init__(self, extension: str, directory: str) -> None:
        super().__init__(
            f"the DuckDB extension {extension} is not installed in {directory};"
            " rebuild the spark-app image"
        )


def s3_secret_statement(lake_read: LakeReadSettings) -> str:
    return (
        "CREATE OR REPLACE SECRET lake_s3 (TYPE s3,"
        f" KEY_ID {literal(lake_read.s3_access_key)},"
        f" SECRET {literal(lake_read.s3_secret_key)},"
        f" ENDPOINT {literal(lake_read.s3_host)},"
        f" URL_STYLE 'path', USE_SSL {str(lake_read.s3_uses_ssl).lower()},"
        f" REGION {literal(lake_read.s3_region)})"
    )


def catalog_attach_statement(catalog: str, lake_read: LakeReadSettings) -> str:
    return (
        f"ATTACH {literal(REST_WAREHOUSE)} AS {identifier(catalog)} (TYPE iceberg,"
        f" URI {literal(lake_read.catalog_uri)},"
        " AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
    )


def postgres_uri(conninfo: str, user: str, password: str) -> str:
    """The libpq URI with the login added as query parameters."""
    separator = "&" if "?" in conninfo else "?"
    login = f"user={quote(user, safe='')}&password={quote(password, safe='')}"
    return f"{conninfo}{separator}{login}"


def postgres_attach_statement(alias: str, uri: str, *, read_only: bool) -> str:
    options = "TYPE postgres, READ_ONLY" if read_only else "TYPE postgres"
    return f"ATTACH {literal(uri)} AS {alias} ({options})"


def setup_statements(settings: LatencySettings) -> list[str]:
    source = settings.source_db
    analytics = settings.analytics_db
    return [
        s3_secret_statement(settings.lake_read),
        catalog_attach_statement(settings.lake.catalog, settings.lake_read),
        postgres_attach_statement(
            SOURCE_ALIAS,
            postgres_uri(source.conninfo(), source.user, source.password),
            read_only=False,
        ),
        postgres_attach_statement(
            GOLD_ALIAS,
            postgres_uri(analytics.conninfo(), analytics.user, analytics.password),
            read_only=True,
        ),
    ]


def connection_config(settings: LatencySettings) -> DuckDbConfig:
    """The extensions come installed in the image, so a missing one fails instead of being
    downloaded."""
    return {
        "extension_directory": settings.extension_directory,
        "autoinstall_known_extensions": False,
    }


def load_extensions(connection: duckdb.DuckDBPyConnection, directory: str) -> None:
    for extension in EXTENSIONS:
        try:
            connection.load_extension(extension)
        except duckdb.IOException as error:
            raise MissingExtensionError(extension, directory) from error


def connect(settings: LatencySettings) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(config=connection_config(settings))
    load_extensions(connection, settings.extension_directory)
    for statement in setup_statements(settings):
        connection.execute(statement)
    return connection


def query_runner(connection: duckdb.DuckDBPyConnection) -> Callable[[str], list[Row]]:
    def run(sql: str) -> list[Row]:
        return connection.execute(sql).fetchall()

    return run


def postgres_query_sql(alias: str, sql: str) -> str:
    return f"SELECT * FROM postgres_query({literal(alias)}, {literal(sql)})"


def postgres_runner(
    connection: duckdb.DuckDBPyConnection, alias: str
) -> Callable[[str], list[Row]]:
    """Runs a statement in the attached PostgreSQL database itself, so its filters and any data
    changes happen there."""

    def run(sql: str) -> list[Row]:
        return connection.execute(postgres_query_sql(alias, sql)).fetchall()

    return run


@dataclass(frozen=True, slots=True)
class DuckDbTextStore:
    connection: duckdb.DuckDBPyConnection

    def write_text(self, path: str, text: str) -> None:
        """DuckDB has no plain text writer: a one-column CSV without quoting or escaping writes
        the value as is, followed by a newline, so a trailing newline is removed first."""
        self.connection.execute(
            f"COPY (SELECT ? AS text) TO {literal(duckdb_path(path))}"
            " (FORMAT csv, HEADER false, QUOTE '', ESCAPE '')",
            [text.removesuffix("\n")],
        )

    def list_files(self, folder: str, suffix: str) -> list[str]:
        pattern = f"{duckdb_path(folder)}/*{suffix}"
        rows = self.connection.execute("SELECT file FROM glob(?) ORDER BY file", [pattern])
        return [str(file) for (file,) in rows.fetchall()]

    def read_text(self, path: str) -> str:
        rows = self.connection.execute("SELECT content FROM read_text(?)", [duckdb_path(path)])
        (content,) = rows.fetchone() or ("",)
        return str(content)


@dataclass(frozen=True, slots=True)
class DuckDbSinkLogStore:
    """The log of a file sink writing to `path`, in its `_spark_metadata` folder."""

    store: DuckDbTextStore
    path: str

    @property
    def folder(self) -> str:
        return f"{self.path}/{SINK_LOG_FOLDER}"

    def log_names(self) -> list[str]:
        return [file.rsplit("/", 1)[-1] for file in self.store.list_files(self.folder, "")]

    def read_log(self, name: str) -> str:
        return self.store.read_text(f"{self.folder}/{name}")
