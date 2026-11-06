from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from common.settings import AnalyticsDbSettings, optional

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_LAKE_ROOT = "s3://lake"
DEFAULT_ICEBERG_CATALOG = "lake"


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        servers = optional(env, "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
        return cls(bootstrap_servers=servers)


@dataclass(frozen=True, slots=True)
class LakeSettings:
    root: str
    checkpoint_root: str
    catalog: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        root = optional(env, "LAKE_ROOT", DEFAULT_LAKE_ROOT).rstrip("/")
        checkpoint_root = optional(env, "CHECKPOINT_ROOT", f"{root}/checkpoints").rstrip("/")
        return cls(
            root=root,
            checkpoint_root=checkpoint_root,
            catalog=optional(env, "ICEBERG_CATALOG", DEFAULT_ICEBERG_CATALOG),
        )


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    kafka: KafkaSettings
    lake: LakeSettings
    analytics_db: AnalyticsDbSettings

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(
            kafka=KafkaSettings.from_env(env),
            lake=LakeSettings.from_env(env),
            analytics_db=AnalyticsDbSettings.from_env(env),
        )
