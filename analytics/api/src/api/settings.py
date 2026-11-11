from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from common.settings import AnalyticsDbSettings, InvalidSettingError, positive_int, require

DEFAULT_POOL_MIN_SIZE = 1
DEFAULT_POOL_MAX_SIZE = 5
DEFAULT_POOL_TIMEOUT_SECONDS = 5


@dataclass(frozen=True, slots=True)
class PoolSettings:
    min_size: int
    max_size: int
    timeout_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        min_size = positive_int(env, "API_DB_POOL_MIN_SIZE", DEFAULT_POOL_MIN_SIZE)
        max_size = positive_int(env, "API_DB_POOL_MAX_SIZE", DEFAULT_POOL_MAX_SIZE)
        if min_size > max_size:
            raise InvalidSettingError(
                "API_DB_POOL_MIN_SIZE", str(min_size), f"at most API_DB_POOL_MAX_SIZE ({max_size})"
            )
        return cls(
            min_size=min_size,
            max_size=max_size,
            timeout_seconds=positive_int(
                env, "API_DB_POOL_TIMEOUT_SECONDS", DEFAULT_POOL_TIMEOUT_SECONDS
            ),
        )


@dataclass(frozen=True, slots=True)
class ApiSettings:
    db: AnalyticsDbSettings
    pool: PoolSettings

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        require(env, *AnalyticsDbSettings.REQUIRED)
        return cls(db=AnalyticsDbSettings.from_env(env), pool=PoolSettings.from_env(env))
