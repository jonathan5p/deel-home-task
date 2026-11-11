import argparse
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Self
from urllib.parse import urlsplit

from common.settings import (
    JDBC_PREFIX,
    AnalyticsDbSettings,
    InvalidSettingError,
    positive_int,
    require,
    split_list,
)
from pipelines.latency.layers import Layer
from pipelines.settings import (
    DEFAULT_BRONZE_TRIGGER_SECONDS,
    DEFAULT_GOLD_TRIGGER_SECONDS,
    DEFAULT_LANDING_TRIGGER_SECONDS,
    DEFAULT_SILVER_TRIGGER_SECONDS,
    LakeSettings,
)

DEFAULT_PROBES = 5
DEFAULT_POLL_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 300.0

TRIGGER_SETTINGS: Mapping[Layer, tuple[str, int]] = {
    Layer.LANDING: ("LANDING_TRIGGER_SECONDS", DEFAULT_LANDING_TRIGGER_SECONDS),
    Layer.BRONZE: ("BRONZE_TRIGGER_SECONDS", DEFAULT_BRONZE_TRIGGER_SECONDS),
    Layer.SILVER: ("SILVER_TRIGGER_SECONDS", DEFAULT_SILVER_TRIGGER_SECONDS),
    Layer.GOLD: ("GOLD_TRIGGER_SECONDS", DEFAULT_GOLD_TRIGGER_SECONDS),
}


@dataclass(frozen=True, slots=True)
class SourceDbSettings:
    REQUIRED: ClassVar[tuple[str, ...]] = ("SOURCE_DB_URL", "SOURCE_DB_USER", "SOURCE_DB_PASSWORD")

    url: str
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        url, user, password = require(env, *cls.REQUIRED)
        return cls(url=url, user=user, password=password)

    def conninfo(self) -> str:
        """The libpq connection URI of the JDBC URL (`jdbc:postgresql://host/db`)."""
        return self.url.removeprefix(JDBC_PREFIX)


@dataclass(frozen=True, slots=True)
class LakeReadSettings:
    """How a reader outside Spark reaches the lake: the Iceberg REST catalog and the object
    storage behind it."""

    REQUIRED: ClassVar[tuple[str, ...]] = (
        "LAKE_CATALOG_URI",
        "LAKE_S3_ENDPOINT",
        "LAKE_S3_REGION",
        "LAKE_S3_ACCESS_KEY",
        "LAKE_S3_SECRET_KEY",
    )

    catalog_uri: str
    s3_endpoint: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        catalog_uri, endpoint, region, access_key, secret_key = require(env, *cls.REQUIRED)
        if urlsplit(endpoint).scheme not in ("http", "https"):
            raise InvalidSettingError("LAKE_S3_ENDPOINT", endpoint, "an http:// or https:// URL")
        return cls(
            catalog_uri=catalog_uri.rstrip("/"),
            s3_endpoint=endpoint.rstrip("/"),
            s3_region=region,
            s3_access_key=access_key,
            s3_secret_key=secret_key,
        )

    @property
    def s3_host(self) -> str:
        return urlsplit(self.s3_endpoint).netloc

    @property
    def s3_uses_ssl(self) -> bool:
        return urlsplit(self.s3_endpoint).scheme == "https"


@dataclass(frozen=True, slots=True)
class LatencySettings:
    topics: tuple[str, ...]
    lake: LakeSettings
    lake_read: LakeReadSettings
    source_db: SourceDbSettings
    analytics_db: AnalyticsDbSettings
    trigger_seconds: Mapping[Layer, int]

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        topics, *_ = require(
            env,
            "KAFKA_TOPICS",
            *LakeSettings.REQUIRED,
            *LakeReadSettings.REQUIRED,
            *SourceDbSettings.REQUIRED,
            *AnalyticsDbSettings.REQUIRED,
        )
        return cls(
            topics=split_list("KAFKA_TOPICS", topics),
            lake=LakeSettings.from_env(env),
            lake_read=LakeReadSettings.from_env(env),
            source_db=SourceDbSettings.from_env(env),
            analytics_db=AnalyticsDbSettings.from_env(env),
            trigger_seconds={
                layer: positive_int(env, name, default)
                for layer, (name, default) in TRIGGER_SETTINGS.items()
            },
        )

    @property
    def longest_trigger_seconds(self) -> int:
        return max(self.trigger_seconds.values())


@dataclass(frozen=True, slots=True)
class ProbeOptions:
    entities: tuple[str, ...]
    probes: int = DEFAULT_PROBES
    poll_seconds: float = DEFAULT_POLL_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_latency: Mapping[Layer, float] = field(default_factory=dict)
    keys: Mapping[str, int] = field(default_factory=dict)


def _positive[T: (int, float)](convert: Callable[[str], T]) -> Callable[[str], T]:
    def parse(text: str) -> T:
        try:
            value = convert(text)
        except ValueError:
            value = None
        if value is None or not math.isfinite(value) or value <= 0:
            raise argparse.ArgumentTypeError(f"{text!r} is not a positive number")
        return value

    return parse


def _pairs(text: str) -> list[tuple[str, str]]:
    pairs = []
    for item in text.split(","):
        name, separator, value = item.partition("=")
        if not separator or not name.strip() or not value.strip():
            raise argparse.ArgumentTypeError(f"{item.strip()!r} is not a <name>=<value> pair")
        pairs.append((name.strip(), value.strip()))
    return pairs


def parse_max_latency(text: str) -> dict[Layer, float]:
    """`<layer>=<seconds>` pairs separated by commas, such as `gold=60,silver=40`."""
    thresholds: dict[Layer, float] = {}
    for name, value in _pairs(text):
        if name not in Layer:
            raise argparse.ArgumentTypeError(
                f"{name!r} is not a layer; use one of {', '.join(Layer)}"
            )
        thresholds[Layer(name)] = _positive(float)(value)
    return thresholds


def _names(known: Sequence[str]) -> Callable[[str], tuple[str, ...]]:
    def parse(text: str) -> tuple[str, ...]:
        names = tuple(name.strip() for name in text.split(",") if name.strip())
        unknown = [name for name in names if name not in known]
        if unknown or not names:
            raise argparse.ArgumentTypeError(
                f"{text!r} is not a list of entities; use some of {', '.join(known)}"
            )
        return tuple(name for name in known if name in names)

    return parse


def _keys(known: Sequence[str]) -> Callable[[str], dict[str, int]]:
    def parse(text: str) -> dict[str, int]:
        keys: dict[str, int] = {}
        for name, value in _pairs(text):
            if name not in known:
                raise argparse.ArgumentTypeError(
                    f"{name!r} is not an entity; use one of {', '.join(known)}"
                )
            try:
                keys[name] = int(value)
            except ValueError as error:
                raise argparse.ArgumentTypeError(f"{value!r} is not an integer key") from error
        return keys

    return parse


def parse_options(argv: Sequence[str], entities: Sequence[str]) -> ProbeOptions:
    """The probe's options from the command line; `entities` are the known entity names, in
    the order the probe reports them."""
    parser = argparse.ArgumentParser(prog="latency-probe")
    parser.add_argument("--entities", type=_names(entities), default=tuple(entities))
    parser.add_argument("--probes", type=_positive(int), default=DEFAULT_PROBES)
    parser.add_argument("--poll-seconds", type=_positive(float), default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--timeout-seconds", type=_positive(float), default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-latency", type=parse_max_latency, default={})
    parser.add_argument("--keys", type=_keys(entities), default={})
    arguments = parser.parse_args(argv)
    return ProbeOptions(
        entities=arguments.entities,
        probes=arguments.probes,
        poll_seconds=arguments.poll_seconds,
        timeout_seconds=arguments.timeout_seconds,
        max_latency=arguments.max_latency,
        keys=arguments.keys,
    )
