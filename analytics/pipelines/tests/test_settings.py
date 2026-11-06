import pytest

from common.settings import AnalyticsDbSettings, MissingSettingsError
from pipelines.settings import KafkaSettings, LakeSettings, PipelineSettings

CREDENTIALS = {"ANALYTICS_DB_USER": "analytics", "ANALYTICS_DB_PASSWORD": "secret"}


def test_kafka_defaults_to_the_local_broker() -> None:
    assert KafkaSettings.from_env({}).bootstrap_servers == "kafka:29092"


def test_kafka_bootstrap_servers_can_be_overridden() -> None:
    settings = KafkaSettings.from_env({"KAFKA_BOOTSTRAP_SERVERS": "broker:9092"})

    assert settings.bootstrap_servers == "broker:9092"


def test_lake_defaults_to_the_local_bucket() -> None:
    settings = LakeSettings.from_env({})

    assert settings.root == "s3://lake"
    assert settings.checkpoint_root == "s3://lake/checkpoints"
    assert settings.catalog == "lake"


def test_checkpoints_follow_an_overridden_lake_root() -> None:
    settings = LakeSettings.from_env({"LAKE_ROOT": "s3://other-lake/", "ICEBERG_CATALOG": "wh"})

    assert settings.root == "s3://other-lake"
    assert settings.checkpoint_root == "s3://other-lake/checkpoints"
    assert settings.catalog == "wh"


def test_checkpoint_root_can_be_set_independently() -> None:
    settings = LakeSettings.from_env({"CHECKPOINT_ROOT": "s3://checkpoints/"})

    assert settings.checkpoint_root == "s3://checkpoints"


def test_pipeline_settings_compose_every_section() -> None:
    env = CREDENTIALS | {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092"}

    settings = PipelineSettings.from_env(env)

    assert settings.kafka == KafkaSettings(bootstrap_servers="broker:9092")
    assert settings.lake == LakeSettings.from_env(env)
    assert settings.analytics_db == AnalyticsDbSettings.from_env(env)


def test_pipeline_settings_require_the_analytics_credentials() -> None:
    with pytest.raises(MissingSettingsError):
        PipelineSettings.from_env({})
