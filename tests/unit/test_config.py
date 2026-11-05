import pytest

from pipeline.config import ConfigError, Settings

REQUIRED = {
    "ANALYTICS_DB_USER": "analytics",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def test_defaults_match_the_local_stack() -> None:
    settings = Settings.from_env(REQUIRED)

    assert settings.kafka_bootstrap_servers == "kafka:29092"
    assert settings.lake_root == "s3://lake"
    assert settings.checkpoint_root == "s3://lake/checkpoints"
    assert settings.catalog == "lake"
    assert settings.analytics_db_url == "jdbc:postgresql://analytics-db:5432/analytics"
    assert settings.analytics_db_user == "analytics"
    assert settings.analytics_db_password == "secret"


def test_environment_overrides_defaults() -> None:
    env = REQUIRED | {
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "LAKE_ROOT": "s3://other-lake/",
        "ICEBERG_CATALOG": "warehouse",
        "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    }

    settings = Settings.from_env(env)

    assert settings.kafka_bootstrap_servers == "broker:9092"
    assert settings.lake_root == "s3://other-lake"
    assert settings.checkpoint_root == "s3://other-lake/checkpoints"
    assert settings.catalog == "warehouse"
    assert settings.analytics_db_url == "jdbc:postgresql://db:5432/gold"


def test_checkpoint_root_can_be_set_independently() -> None:
    settings = Settings.from_env(REQUIRED | {"CHECKPOINT_ROOT": "s3://checkpoints/"})

    assert settings.checkpoint_root == "s3://checkpoints"


def test_missing_required_variables_are_all_reported() -> None:
    with pytest.raises(ConfigError, match="ANALYTICS_DB_USER, ANALYTICS_DB_PASSWORD"):
        Settings.from_env({})


def test_empty_values_count_as_missing() -> None:
    with pytest.raises(ConfigError, match="ANALYTICS_DB_PASSWORD"):
        Settings.from_env(REQUIRED | {"ANALYTICS_DB_PASSWORD": ""})


def test_password_is_hidden_from_repr() -> None:
    assert "secret" not in repr(Settings.from_env(REQUIRED))


def test_reads_process_environment_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)

    assert Settings.from_env().analytics_db_user == "analytics"
