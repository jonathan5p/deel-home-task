import pytest

from api.settings import ApiSettings, PoolSettings
from common.settings import InvalidSettingError, MissingSettingsError

DATABASE = {
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    "ANALYTICS_DB_USER": "reader",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def test_api_settings_read_the_database_and_default_the_pool() -> None:
    settings = ApiSettings.from_env(DATABASE)

    assert settings.db.conninfo() == "postgresql://db:5432/gold"
    assert settings.db.user == "reader"
    assert settings.pool == PoolSettings(min_size=1, max_size=5, timeout_seconds=5)


def test_the_pool_is_tuned_from_the_environment() -> None:
    settings = ApiSettings.from_env(
        DATABASE
        | {
            "API_DB_POOL_MIN_SIZE": "2",
            "API_DB_POOL_MAX_SIZE": "8",
            "API_DB_POOL_TIMEOUT_SECONDS": "3",
        }
    )

    assert settings.pool == PoolSettings(min_size=2, max_size=8, timeout_seconds=3)


def test_every_missing_database_variable_is_reported_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        ApiSettings.from_env({})

    assert error.value.names == ("ANALYTICS_DB_URL", "ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD")


@pytest.mark.parametrize(
    "name", ["API_DB_POOL_MIN_SIZE", "API_DB_POOL_MAX_SIZE", "API_DB_POOL_TIMEOUT_SECONDS"]
)
@pytest.mark.parametrize("value", ["0", "-1", "many"])
def test_a_pool_setting_must_be_a_positive_integer(name: str, value: str) -> None:
    with pytest.raises(InvalidSettingError, match=name):
        ApiSettings.from_env(DATABASE | {name: value})


def test_the_pool_cannot_keep_more_connections_than_it_may_open() -> None:
    with pytest.raises(InvalidSettingError, match="API_DB_POOL_MIN_SIZE"):
        ApiSettings.from_env(DATABASE | {"API_DB_POOL_MIN_SIZE": "6"})


def test_the_password_is_left_out_of_the_settings_repr() -> None:
    assert "secret" not in repr(ApiSettings.from_env(DATABASE))
