import pytest

from common.settings import AnalyticsDbSettings, InvalidSettingError, MissingSettingsError
from pipelines.settings import KafkaSettings, LakeSettings, LandingSettings, PipelineSettings

KAFKA = {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092", "KAFKA_TOPICS": "topic-a, topic-b"}
LAKE = {"LAKE_ROOT": "s3a://bucket/", "ICEBERG_CATALOG": "catalog"}
DATABASE = {
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/gold",
    "ANALYTICS_DB_USER": "user",
    "ANALYTICS_DB_PASSWORD": "secret",
}


def test_kafka_settings_read_the_brokers_and_topics() -> None:
    settings = KafkaSettings.from_env(KAFKA)

    assert settings == KafkaSettings(bootstrap_servers="broker:9092", topics=("topic-a", "topic-b"))


def test_kafka_settings_require_the_brokers_and_topics() -> None:
    with pytest.raises(MissingSettingsError) as error:
        KafkaSettings.from_env({})

    assert error.value.names == ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPICS")


def test_kafka_settings_reject_an_empty_topic_list() -> None:
    with pytest.raises(InvalidSettingError, match="KAFKA_TOPICS"):
        KafkaSettings.from_env(KAFKA | {"KAFKA_TOPICS": " , "})


def test_lake_settings_read_the_root_and_catalog() -> None:
    settings = LakeSettings.from_env(LAKE)

    assert settings.root == "s3a://bucket"
    assert settings.checkpoint_root == "s3a://bucket/checkpoints"
    assert settings.catalog == "catalog"


def test_checkpoint_root_can_be_set_independently() -> None:
    settings = LakeSettings.from_env(LAKE | {"CHECKPOINT_ROOT": "s3a://checkpoints/"})

    assert settings.checkpoint_root == "s3a://checkpoints"


def test_lake_settings_require_the_root_and_catalog() -> None:
    with pytest.raises(MissingSettingsError) as error:
        LakeSettings.from_env({})

    assert error.value.names == ("LAKE_ROOT", "ICEBERG_CATALOG")


def test_landing_settings_need_no_database_credentials() -> None:
    settings = LandingSettings.from_env(KAFKA | LAKE)

    assert settings.kafka == KafkaSettings.from_env(KAFKA)
    assert settings.lake == LakeSettings.from_env(LAKE)
    assert settings.trigger_seconds == 10


def test_landing_trigger_interval_can_be_overridden() -> None:
    settings = LandingSettings.from_env(KAFKA | LAKE | {"LANDING_TRIGGER_SECONDS": "30"})

    assert settings.trigger_seconds == 30


def test_landing_paths_use_one_folder_per_topic() -> None:
    settings = LandingSettings.from_env(KAFKA | LAKE)

    assert settings.landing_path("topic-a") == "s3a://bucket/landing/topic-a"
    assert settings.checkpoint_path("topic-a") == "s3a://bucket/checkpoints/landing/topic-a"


def test_landing_settings_report_every_missing_variable_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        LandingSettings.from_env({})

    assert error.value.names == KafkaSettings.REQUIRED + LakeSettings.REQUIRED


def test_pipeline_settings_compose_every_section() -> None:
    env = KAFKA | LAKE | DATABASE

    settings = PipelineSettings.from_env(env)

    assert settings.kafka == KafkaSettings.from_env(env)
    assert settings.lake == LakeSettings.from_env(env)
    assert settings.analytics_db == AnalyticsDbSettings.from_env(env)


def test_pipeline_settings_report_every_missing_variable_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        PipelineSettings.from_env(KAFKA)

    assert error.value.names == LakeSettings.REQUIRED + AnalyticsDbSettings.REQUIRED
