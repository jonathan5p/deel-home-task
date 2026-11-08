from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Self

DEFAULT_ANALYTICS_DB_URL = "jdbc:postgresql://analytics-db:5432/analytics"


class MissingSettingsError(ValueError):
    def __init__(self, names: Iterable[str]) -> None:
        self.names = tuple(names)
        super().__init__(f"Missing required environment variables: {', '.join(self.names)}")


def require(env: Mapping[str, str], *names: str) -> tuple[str, ...]:
    """Return the values of the given variables, reporting every missing or empty one at once."""
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise MissingSettingsError(missing)
    return tuple(env[name] for name in names)


class InvalidSettingError(ValueError):
    def __init__(self, name: str, value: str, expected: str) -> None:
        super().__init__(f"Environment variable {name}={value!r} is not {expected}")


def optional(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(name) or default


def positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if not value:
        return default
    if not value.isdigit() or int(value) == 0:
        raise InvalidSettingError(name, value, "a positive integer")
    return int(value)


def comma_separated(env: Mapping[str, str], name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = env.get(name)
    if not value:
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class AnalyticsDbSettings:
    url: str
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        user, password = require(env, "ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD")
        return cls(
            url=optional(env, "ANALYTICS_DB_URL", DEFAULT_ANALYTICS_DB_URL),
            user=user,
            password=password,
        )
