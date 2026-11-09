import pytest

from common.settings import AnalyticsDbSettings, InvalidSettingError, MissingSettingsError
from pipelines.settings import (
    BronzeSettings,
    KafkaSettings,
    LakeSettings,
    LandingQualitySettings,
    LandingSettings,
    PipelineSettings,
)

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


def test_bronze_settings_need_only_the_topics_and_the_lake() -> None:
    settings = BronzeSettings.from_env({"KAFKA_TOPICS": "topic-a, topic-b"} | LAKE)

    assert settings.topics == ("topic-a", "topic-b")
    assert settings.lake == LakeSettings.from_env(LAKE)
    assert settings.trigger_seconds == 10


def test_bronze_trigger_interval_can_be_overridden() -> None:
    settings = BronzeSettings.from_env(KAFKA | LAKE | {"BRONZE_TRIGGER_SECONDS": "30"})

    assert settings.trigger_seconds == 30


@pytest.mark.parametrize("value", ["0", "-5", "ten", "1.5"])
def test_bronze_trigger_interval_must_be_a_positive_integer(value: str) -> None:
    with pytest.raises(InvalidSettingError, match="BRONZE_TRIGGER_SECONDS"):
        BronzeSettings.from_env(KAFKA | LAKE | {"BRONZE_TRIGGER_SECONDS": value})


def test_bronze_settings_report_every_missing_variable_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        BronzeSettings.from_env({})

    assert error.value.names == ("KAFKA_TOPICS", *LakeSettings.REQUIRED)


def test_bronze_reads_the_landing_folders_and_writes_one_namespace_per_catalog() -> None:
    settings = BronzeSettings.from_env(KAFKA | LAKE)

    assert settings.landing_path("topic-a") == LandingSettings.from_env(KAFKA | LAKE).landing_path(
        "topic-a"
    )
    assert settings.table("customers") == "catalog.bronze.customers"
    assert settings.checkpoint_path("customers") == "s3a://bucket/checkpoints/bronze/customers"


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


def test_landing_quality_checks_everything_landed_by_default() -> None:
    settings = LandingQualitySettings.from_env(KAFKA | LAKE)

    assert settings.landing == LandingSettings.from_env(KAFKA | LAKE)
    assert settings.last_offsets is None


def test_landing_quality_window_can_be_limited_to_the_latest_offsets() -> None:
    settings = LandingQualitySettings.from_env(
        KAFKA | LAKE | {"LANDING_CHECK_LAST_OFFSETS": "1000"}
    )

    assert settings.last_offsets == 1000
