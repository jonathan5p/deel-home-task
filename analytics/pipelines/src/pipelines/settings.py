from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, Self

from common.settings import (
    AnalyticsDbSettings,
    optional,
    optional_positive_int,
    positive_int,
    require,
    split_list,
)

DEFAULT_LANDING_TRIGGER_SECONDS = 10
DEFAULT_BRONZE_TRIGGER_SECONDS = 10
DEFAULT_SILVER_TRIGGER_SECONDS = 10


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    REQUIRED: ClassVar[tuple[str, ...]] = ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPICS")

    bootstrap_servers: str
    topics: tuple[str, ...]

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        servers, topics = require(env, *cls.REQUIRED)
        return cls(bootstrap_servers=servers, topics=split_list("KAFKA_TOPICS", topics))


@dataclass(frozen=True, slots=True)
class LakeSettings:
    REQUIRED: ClassVar[tuple[str, ...]] = ("LAKE_ROOT", "ICEBERG_CATALOG")

    root: str
    checkpoint_root: str
    catalog: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        root, catalog = require(env, *cls.REQUIRED)
        root = root.rstrip("/")
        checkpoint_root = optional(env, "CHECKPOINT_ROOT", f"{root}/checkpoints").rstrip("/")
        return cls(root=root, checkpoint_root=checkpoint_root, catalog=catalog)

    def landing_path(self, topic: str) -> str:
        return f"{self.root}/landing/{topic}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"

    def checkpoint_path(self, layer: str, name: str) -> str:
        return f"{self.checkpoint_root}/{layer}/{name}"


@dataclass(frozen=True, slots=True)
class LandingSettings:
    kafka: KafkaSettings
    lake: LakeSettings
    trigger_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        require(env, *KafkaSettings.REQUIRED, *LakeSettings.REQUIRED)
        return cls(
            kafka=KafkaSettings.from_env(env),
            lake=LakeSettings.from_env(env),
            trigger_seconds=positive_int(
                env, "LANDING_TRIGGER_SECONDS", DEFAULT_LANDING_TRIGGER_SECONDS
            ),
        )

    def landing_path(self, topic: str) -> str:
        return self.lake.landing_path(topic)

    def checkpoint_path(self, topic: str) -> str:
        return f"{self.lake.checkpoint_root}/landing/{topic}"


@dataclass(frozen=True, slots=True)
class BronzeSettings:
    LAYER: ClassVar[str] = "bronze"

    topics: tuple[str, ...]
    lake: LakeSettings
    trigger_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        topics, *_ = require(env, "KAFKA_TOPICS", *LakeSettings.REQUIRED)
        return cls(
            topics=split_list("KAFKA_TOPICS", topics),
            lake=LakeSettings.from_env(env),
            trigger_seconds=positive_int(
                env, "BRONZE_TRIGGER_SECONDS", DEFAULT_BRONZE_TRIGGER_SECONDS
            ),
        )

    def landing_path(self, topic: str) -> str:
        return self.lake.landing_path(topic)

    def table(self, name: str) -> str:
        return self.lake.table(self.LAYER, name)

    def checkpoint_path(self, name: str) -> str:
        return self.lake.checkpoint_path(self.LAYER, name)


@dataclass(frozen=True, slots=True)
class SilverSettings:
    LAYER: ClassVar[str] = "silver"

    topics: tuple[str, ...]
    lake: LakeSettings
    trigger_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        topics, *_ = require(env, "KAFKA_TOPICS", *LakeSettings.REQUIRED)
        return cls(
            topics=split_list("KAFKA_TOPICS", topics),
            lake=LakeSettings.from_env(env),
            trigger_seconds=positive_int(
                env, "SILVER_TRIGGER_SECONDS", DEFAULT_SILVER_TRIGGER_SECONDS
            ),
        )

    def bronze_table(self, name: str) -> str:
        return self.lake.table(BronzeSettings.LAYER, name)

    def table(self, name: str) -> str:
        return self.lake.table(self.LAYER, name)

    def checkpoint_path(self, name: str) -> str:
        return self.lake.checkpoint_path(self.LAYER, name)


@dataclass(frozen=True, slots=True)
class LandingQualitySettings:
    landing: LandingSettings
    last_offsets: int | None

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(
            landing=LandingSettings.from_env(env),
            last_offsets=optional_positive_int(env, "LANDING_CHECK_LAST_OFFSETS"),
        )


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    kafka: KafkaSettings
    lake: LakeSettings
    analytics_db: AnalyticsDbSettings

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        require(env, *KafkaSettings.REQUIRED, *LakeSettings.REQUIRED, *AnalyticsDbSettings.REQUIRED)
        return cls(
            kafka=KafkaSettings.from_env(env),
            lake=LakeSettings.from_env(env),
            analytics_db=AnalyticsDbSettings.from_env(env),
        )
