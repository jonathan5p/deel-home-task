import pytest

from common.settings import (
    AnalyticsDbSettings,
    InvalidSettingError,
    MissingSettingsError,
    comma_separated,
    positive_int,
)

CREDENTIALS = {"ANALYTICS_DB_USER": "analytics", "ANALYTICS_DB_PASSWORD": "secret"}


def test_url_defaults_to_the_local_analytics_database() -> None:
    settings = AnalyticsDbSettings.from_env(CREDENTIALS)

    assert settings.url == "jdbc:postgresql://analytics-db:5432/analytics"
    assert settings.user == "analytics"
    assert settings.password == "secret"


def test_url_can_be_overridden() -> None:
    env = CREDENTIALS | {"ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold"}

    assert AnalyticsDbSettings.from_env(env).url == "jdbc:postgresql://db:5432/gold"


def test_missing_credentials_are_all_reported() -> None:
    with pytest.raises(MissingSettingsError) as error:
        AnalyticsDbSettings.from_env({})

    assert error.value.names == ("ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD")
    assert "ANALYTICS_DB_USER, ANALYTICS_DB_PASSWORD" in str(error.value)


def test_empty_values_count_as_missing() -> None:
    with pytest.raises(MissingSettingsError, match="ANALYTICS_DB_PASSWORD"):
        AnalyticsDbSettings.from_env(CREDENTIALS | {"ANALYTICS_DB_PASSWORD": ""})


def test_password_is_hidden_from_repr() -> None:
    assert "secret" not in repr(AnalyticsDbSettings.from_env(CREDENTIALS))


def test_positive_int_uses_the_default_when_unset() -> None:
    assert positive_int({}, "TRIGGER_SECONDS", 10) == 10


def test_positive_int_parses_the_variable() -> None:
    assert positive_int({"TRIGGER_SECONDS": "30"}, "TRIGGER_SECONDS", 10) == 30


@pytest.mark.parametrize("value", ["0", "-5", "ten", "1.5"])
def test_positive_int_rejects_values_that_are_not_positive_integers(value: str) -> None:
    with pytest.raises(InvalidSettingError, match="TRIGGER_SECONDS"):
        positive_int({"TRIGGER_SECONDS": value}, "TRIGGER_SECONDS", 10)


def test_comma_separated_uses_the_default_when_unset() -> None:
    assert comma_separated({}, "TOPICS", ("a", "b")) == ("a", "b")


def test_comma_separated_splits_and_trims_values() -> None:
    assert comma_separated({"TOPICS": " a, b ,,c "}, "TOPICS", ("x",)) == ("a", "b", "c")
