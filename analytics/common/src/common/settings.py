from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import ClassVar, Self

JDBC_PREFIX = "jdbc:"


class MissingSettingsError(ValueError):
    def __init__(self, names: Iterable[str]) -> None:
        self.names = tuple(names)
        super().__init__(f"Missing required environment variables: {', '.join(self.names)}")


class InvalidSettingError(ValueError):
    def __init__(self, name: str, value: str, expected: str) -> None:
        super().__init__(f"Environment variable {name}={value!r} is not {expected}")


def require(env: Mapping[str, str], *names: str) -> tuple[str, ...]:
    """Return the values of the given variables, reporting every missing or empty one at once."""
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise MissingSettingsError(missing)
    return tuple(env[name] for name in names)


def optional(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(name) or default


def optional_positive_int(env: Mapping[str, str], name: str) -> int | None:
    value = env.get(name)
    if not value:
        return None
    if not value.isdecimal() or int(value) == 0:
        raise InvalidSettingError(name, value, "a positive integer")
    return int(value)


def positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    value = optional_positive_int(env, name)
    return default if value is None else value


def split_list(name: str, value: str) -> tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    if not items:
        raise InvalidSettingError(name, value, "a comma-separated list")
    return items


@dataclass(frozen=True, slots=True)
class AnalyticsDbSettings:
    REQUIRED: ClassVar[tuple[str, ...]] = (
        "ANALYTICS_DB_URL",
        "ANALYTICS_DB_USER",
        "ANALYTICS_DB_PASSWORD",
    )

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
