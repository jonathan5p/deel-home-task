from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from common.settings import AnalyticsDbSettings, comma_separated, optional, positive_int

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_LAKE_ROOT = "s3a://lake"
DEFAULT_ICEBERG_CATALOG = "lake"
DEFAULT_LANDING_TRIGGER_SECONDS = 10
CDC_TOPICS = (
    "finance_db.operations.customers",
    "finance_db.operations.products",
    "finance_db.operations.orders",
    "finance_db.operations.order_items",
)


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str
    topics: tuple[str, ...]

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(
            bootstrap_servers=optional(
                env, "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
            ),
            topics=comma_separated(env, "KAFKA_TOPICS", CDC_TOPICS),
        )


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
class LandingSettings:
    kafka: KafkaSettings
    lake: LakeSettings
    trigger_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(
            kafka=KafkaSettings.from_env(env),
            lake=LakeSettings.from_env(env),
            trigger_seconds=positive_int(
                env, "LANDING_TRIGGER_SECONDS", DEFAULT_LANDING_TRIGGER_SECONDS
            ),
        )

    def landing_path(self, topic: str) -> str:
        return f"{self.lake.root}/landing/{topic}"

    def checkpoint_path(self, topic: str) -> str:
        return f"{self.lake.checkpoint_root}/landing/{topic}"


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
