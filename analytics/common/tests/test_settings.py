import pytest

from common.settings import (
    AnalyticsDbSettings,
    InvalidSettingError,
    MissingSettingsError,
    optional_positive_int,
    positive_int,
    split_list,
)

DATABASE = {
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    "ANALYTICS_DB_USER": "user",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def test_analytics_db_settings_read_the_environment() -> None:
    settings = AnalyticsDbSettings.from_env(DATABASE)

    assert settings.url == "jdbc:postgresql://db:5432/gold"
    assert settings.user == "user"
    assert settings.password == "secret"


def test_every_missing_analytics_db_variable_is_reported_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        AnalyticsDbSettings.from_env({})

    assert error.value.names == AnalyticsDbSettings.REQUIRED
    assert "ANALYTICS_DB_URL, ANALYTICS_DB_USER, ANALYTICS_DB_PASSWORD" in str(error.value)


def test_empty_values_count_as_missing() -> None:
    with pytest.raises(MissingSettingsError, match="ANALYTICS_DB_PASSWORD"):
        AnalyticsDbSettings.from_env(DATABASE | {"ANALYTICS_DB_PASSWORD": ""})


def test_password_is_hidden_from_repr() -> None:
    assert "secret" not in repr(AnalyticsDbSettings.from_env(DATABASE))


def test_positive_int_uses_the_default_when_unset() -> None:
    assert positive_int({}, "TRIGGER_SECONDS", 10) == 10


def test_positive_int_parses_the_variable() -> None:
    assert positive_int({"TRIGGER_SECONDS": "30"}, "TRIGGER_SECONDS", 10) == 30


@pytest.mark.parametrize("value", ["0", "-5", "ten", "1.5"])
def test_positive_int_rejects_values_that_are_not_positive_integers(value: str) -> None:
    with pytest.raises(InvalidSettingError, match="TRIGGER_SECONDS"):
        positive_int({"TRIGGER_SECONDS": value}, "TRIGGER_SECONDS", 10)


def test_split_list_splits_and_trims_values() -> None:
    assert split_list("TOPICS", " a, b ,,c ") == ("a", "b", "c")


def test_split_list_rejects_a_list_without_items() -> None:
    with pytest.raises(InvalidSettingError, match="TOPICS"):
        split_list("TOPICS", " , ,")


def test_optional_positive_int_is_none_when_unset() -> None:
    assert optional_positive_int({}, "WINDOW") is None


def test_optional_positive_int_parses_the_variable() -> None:
    assert optional_positive_int({"WINDOW": "500"}, "WINDOW") == 500


def test_optional_positive_int_rejects_zero() -> None:
    with pytest.raises(InvalidSettingError, match="WINDOW"):
        optional_positive_int({"WINDOW": "0"}, "WINDOW")
