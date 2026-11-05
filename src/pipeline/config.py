import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Self

DEFAULTS: Mapping[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
    "LAKE_ROOT": "s3://lake",
    "ICEBERG_CATALOG": "lake",
    "ANALYTICS_DB_URL": "jdbc:postgresql://analytics-db:5432/analytics",
}
REQUIRED: tuple[str, ...] = ("ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD")


class ConfigError(ValueError):
    """Raised when required settings are missing."""


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_bootstrap_servers: str
    lake_root: str
    checkpoint_root: str
    catalog: str
    analytics_db_url: str
    analytics_db_user: str
    analytics_db_password: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        source = os.environ if env is None else env
        missing = [name for name in REQUIRED if not source.get(name)]
        if missing:
            raise ConfigError(f"Missing required environment variables: {', '.join(missing)}")

        def value(name: str) -> str:
            return source.get(name) or DEFAULTS[name]

        lake_root = value("LAKE_ROOT").rstrip("/")
        checkpoint_root = source.get("CHECKPOINT_ROOT") or f"{lake_root}/checkpoints"
        return cls(
            kafka_bootstrap_servers=value("KAFKA_BOOTSTRAP_SERVERS"),
            lake_root=lake_root,
            checkpoint_root=checkpoint_root.rstrip("/"),
            catalog=value("ICEBERG_CATALOG"),
            analytics_db_url=value("ANALYTICS_DB_URL"),
            analytics_db_user=source["ANALYTICS_DB_USER"],
            analytics_db_password=source["ANALYTICS_DB_PASSWORD"],
        )
