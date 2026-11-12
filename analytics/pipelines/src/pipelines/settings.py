from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar, Self

from common.settings import (
    AnalyticsDbSettings,
    InvalidSettingError,
    optional,
    optional_positive_int,
    positive_int,
    require,
    split_list,
)
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS
from pipelines.schemas.definitions import load_entities

DEFAULT_LANDING_TRIGGER_SECONDS = 10
DEFAULT_BRONZE_TRIGGER_SECONDS = 10
DEFAULT_SILVER_TRIGGER_SECONDS = 10
DEFAULT_GOLD_TRIGGER_SECONDS = 10
DEFAULT_MAINTENANCE_NAMESPACES = ("bronze", "silver")
DEFAULT_SNAPSHOT_RETENTION_HOURS = 168
DEFAULT_RETAIN_LAST = 10
DEFAULT_ORPHAN_AGE_HOURS = 72
MIN_ORPHAN_AGE_HOURS = 24
DEFAULT_GOLD_RETENTION_MONTHS = 36
DEFAULT_GOLD_PARTITION_MONTHS_AHEAD = 2
DEFAULT_STAGE_EXPORTER_POLL_SECONDS = 30
DEFAULT_TABLE_STATS_NAMESPACES = ("bronze", "silver")
DEFAULT_SMALL_FILE_BYTES = 32 * 1024 * 1024
DEFAULT_TABLE_STATS_INTERVAL_SECONDS = 300
DEFAULT_PARTITION_STATS_INTERVAL_SECONDS = 3600


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


class LakeLayerNames:
    """The names of the lake tables and checkpoints of the layer `LAYER`, for the settings of
    every layer, which hold the `lake` they are named in."""

    __slots__ = ()

    LAYER: ClassVar[str]
    lake: LakeSettings

    def table(self, name: str) -> str:
        return self.lake.table(self.LAYER, name)

    def checkpoint_path(self, name: str) -> str:
        return self.lake.checkpoint_path(self.LAYER, name)


class SourcedLakeLayerNames(LakeLayerNames):
    """The names of a layer that also reads the tables of the layer `SOURCE_LAYER`."""

    __slots__ = ()

    SOURCE_LAYER: ClassVar[str]

    def source_table(self, name: str) -> str:
        return self.lake.table(self.SOURCE_LAYER, name)


@dataclass(frozen=True, slots=True)
class LandingSettings:
    LAYER: ClassVar[str] = "landing"

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
        return self.lake.checkpoint_path(self.LAYER, topic)


@dataclass(frozen=True, slots=True)
class BronzeSettings(LakeLayerNames):
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


@dataclass(frozen=True, slots=True)
class SilverSettings(SourcedLakeLayerNames):
    LAYER: ClassVar[str] = "silver"
    SOURCE_LAYER: ClassVar[str] = BronzeSettings.LAYER

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


@dataclass(frozen=True, slots=True)
class GoldSettings(SourcedLakeLayerNames):
    LAYER: ClassVar[str] = "gold"
    SOURCE_LAYER: ClassVar[str] = SilverSettings.LAYER

    lake: LakeSettings
    analytics_db: AnalyticsDbSettings
    trigger_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        require(env, *LakeSettings.REQUIRED, *AnalyticsDbSettings.REQUIRED)
        return cls(
            lake=LakeSettings.from_env(env),
            analytics_db=AnalyticsDbSettings.from_env(env),
            trigger_seconds=positive_int(env, "GOLD_TRIGGER_SECONDS", DEFAULT_GOLD_TRIGGER_SECONDS),
        )


def downstream_checkpoints(value: str) -> dict[str, tuple[str, ...]]:
    """Parse `<namespace>.<table>=<checkpoint>[|<checkpoint>...]` entries separated by commas."""
    streams: dict[str, tuple[str, ...]] = {}
    for entry in split_list("MAINTENANCE_DOWNSTREAM_CHECKPOINTS", value):
        table, separator, paths = entry.partition("=")
        checkpoints = tuple(path.strip() for path in paths.split("|") if path.strip())
        if not separator or "." not in table.strip() or not checkpoints:
            raise InvalidSettingError(
                "MAINTENANCE_DOWNSTREAM_CHECKPOINTS",
                value,
                "a list of <namespace>.<table>=<checkpoint>[|<checkpoint>...] entries",
            )
        streams[table.strip()] = checkpoints
    return streams


def default_downstream(lake: LakeSettings) -> dict[str, tuple[str, ...]]:
    """The checkpoints of the silver and gold streams, by the `<namespace>.<table>` they read."""
    streams: dict[str, tuple[str, ...]] = {
        f"{BronzeSettings.LAYER}.{entity.name}": (
            lake.checkpoint_path(SilverSettings.LAYER, entity.name),
        )
        for entity in load_entities()
    }
    for load in (*DIMENSIONS, *FACTS):
        table = f"{SilverSettings.LAYER}.{load.entity.name}"
        streams[table] = (
            *streams.get(table, ()),
            lake.checkpoint_path(GoldSettings.LAYER, load.name),
        )
    return streams


@dataclass(frozen=True, slots=True)
class MaintenanceSettings:
    catalog: str
    namespaces: tuple[str, ...]
    snapshot_retention_hours: int
    retain_last: int
    orphan_age_hours: int
    downstream: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        require(env, *LakeSettings.REQUIRED)
        lake = LakeSettings.from_env(env)
        namespaces = env.get("MAINTENANCE_NAMESPACES")
        downstream = env.get("MAINTENANCE_DOWNSTREAM_CHECKPOINTS")
        orphan_age_hours = positive_int(
            env, "MAINTENANCE_ORPHAN_AGE_HOURS", DEFAULT_ORPHAN_AGE_HOURS
        )
        if orphan_age_hours < MIN_ORPHAN_AGE_HOURS:
            raise InvalidSettingError(
                "MAINTENANCE_ORPHAN_AGE_HOURS",
                str(orphan_age_hours),
                f"at least {MIN_ORPHAN_AGE_HOURS}",
            )
        return cls(
            catalog=lake.catalog,
            namespaces=(
                split_list("MAINTENANCE_NAMESPACES", namespaces)
                if namespaces
                else DEFAULT_MAINTENANCE_NAMESPACES
            ),
            snapshot_retention_hours=positive_int(
                env, "MAINTENANCE_SNAPSHOT_RETENTION_HOURS", DEFAULT_SNAPSHOT_RETENTION_HOURS
            ),
            retain_last=positive_int(env, "MAINTENANCE_RETAIN_LAST", DEFAULT_RETAIN_LAST),
            orphan_age_hours=orphan_age_hours,
            downstream=(
                downstream_checkpoints(downstream) if downstream else default_downstream(lake)
            ),
        )

    def checkpoints(self, table: str) -> tuple[str, ...]:
        """The checkpoints of the streams that read `table` (`<namespace>.<table>`)."""
        return self.downstream.get(table, ())


@dataclass(frozen=True, slots=True)
class GoldRetentionSettings:
    analytics_db: AnalyticsDbSettings
    keep_months: int
    months_ahead: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(
            analytics_db=AnalyticsDbSettings.from_env(env),
            keep_months=positive_int(env, "GOLD_RETENTION_MONTHS", DEFAULT_GOLD_RETENTION_MONTHS),
            months_ahead=positive_int(
                env, "GOLD_PARTITION_MONTHS_AHEAD", DEFAULT_GOLD_PARTITION_MONTHS_AHEAD
            ),
        )


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


@dataclass(frozen=True, slots=True)
class StageExporterSettings:
    history_server_url: str
    port: int
    poll_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        url, _ = require(env, "HISTORY_SERVER_URL", "STAGE_EXPORTER_PORT")
        return cls(
            history_server_url=url,
            port=positive_int(env, "STAGE_EXPORTER_PORT", 0),
            poll_seconds=positive_int(
                env, "STAGE_EXPORTER_POLL_SECONDS", DEFAULT_STAGE_EXPORTER_POLL_SECONDS
            ),
        )


@dataclass(frozen=True, slots=True)
class TableStatsSettings:
    REQUIRED: ClassVar[tuple[str, ...]] = (
        "LAKE_CATALOG_URI",
        "ICEBERG_CATALOG",
        "LAKE_S3_ENDPOINT",
        "LAKE_S3_REGION",
        "LAKE_S3_ACCESS_KEY",
        "LAKE_S3_SECRET_KEY",
    )

    catalog_uri: str
    catalog_name: str
    s3_endpoint: str
    s3_region: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    namespaces: tuple[str, ...]
    small_file_bytes: int
    table_interval_seconds: int
    partition_interval_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        uri, name, endpoint, region, access_key, secret_key = require(env, *cls.REQUIRED)
        return cls(
            catalog_uri=uri,
            catalog_name=name,
            s3_endpoint=endpoint,
            s3_region=region,
            s3_access_key=access_key,
            s3_secret_key=secret_key,
            namespaces=split_list(
                "TABLE_STATS_NAMESPACES",
                optional(env, "TABLE_STATS_NAMESPACES", ",".join(DEFAULT_TABLE_STATS_NAMESPACES)),
            ),
            small_file_bytes=positive_int(env, "SMALL_FILE_BYTES", DEFAULT_SMALL_FILE_BYTES),
            table_interval_seconds=positive_int(
                env, "TABLE_STATS_INTERVAL_SECONDS", DEFAULT_TABLE_STATS_INTERVAL_SECONDS
            ),
            partition_interval_seconds=positive_int(
                env, "PARTITION_STATS_INTERVAL_SECONDS", DEFAULT_PARTITION_STATS_INTERVAL_SECONDS
            ),
        )

    def catalog_properties(self) -> dict[str, str]:
        """PyIceberg's properties for the REST catalog and the S3 files its tables point at."""
        return {
            "type": "rest",
            "uri": self.catalog_uri,
            "s3.endpoint": self.s3_endpoint,
            "s3.region": self.s3_region,
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "s3.force-virtual-addressing": "false",
        }
