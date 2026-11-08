import pytest

from common.settings import AnalyticsDbSettings, MissingSettingsError
from pipelines.settings import (
    CDC_TOPICS,
    KafkaSettings,
    LakeSettings,
    LandingSettings,
    PipelineSettings,
)

CREDENTIALS = {"ANALYTICS_DB_USER": "analytics", "ANALYTICS_DB_PASSWORD": "secret"}


def test_kafka_defaults_to_the_local_broker_and_the_cdc_topics() -> None:
    settings = KafkaSettings.from_env({})

    assert settings.bootstrap_servers == "kafka:29092"
    assert settings.topics == CDC_TOPICS
    assert CDC_TOPICS == (
        "finance_db.operations.customers",
        "finance_db.operations.products",
        "finance_db.operations.orders",
        "finance_db.operations.order_items",
    )


def test_kafka_topics_can_be_overridden() -> None:
    settings = KafkaSettings.from_env({"KAFKA_TOPICS": "orders, order_items"})

    assert settings.topics == ("orders", "order_items")


def test_kafka_bootstrap_servers_can_be_overridden() -> None:
    settings = KafkaSettings.from_env({"KAFKA_BOOTSTRAP_SERVERS": "broker:9092"})

    assert settings.bootstrap_servers == "broker:9092"


def test_lake_defaults_to_the_local_bucket() -> None:
    settings = LakeSettings.from_env({})

    assert settings.root == "s3a://lake"
    assert settings.checkpoint_root == "s3a://lake/checkpoints"
    assert settings.catalog == "lake"


def test_checkpoints_follow_an_overridden_lake_root() -> None:
    settings = LakeSettings.from_env({"LAKE_ROOT": "s3a://other-lake/", "ICEBERG_CATALOG": "wh"})

    assert settings.root == "s3a://other-lake"
    assert settings.checkpoint_root == "s3a://other-lake/checkpoints"
    assert settings.catalog == "wh"


def test_checkpoint_root_can_be_set_independently() -> None:
    settings = LakeSettings.from_env({"CHECKPOINT_ROOT": "s3a://checkpoints/"})

    assert settings.checkpoint_root == "s3a://checkpoints"


def test_pipeline_settings_compose_every_section() -> None:
    env = CREDENTIALS | {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092"}

    settings = PipelineSettings.from_env(env)

    assert settings.kafka == KafkaSettings(bootstrap_servers="broker:9092", topics=CDC_TOPICS)
    assert settings.lake == LakeSettings.from_env(env)
    assert settings.analytics_db == AnalyticsDbSettings.from_env(env)


def test_pipeline_settings_require_the_analytics_credentials() -> None:
    with pytest.raises(MissingSettingsError):
        PipelineSettings.from_env({})


def test_landing_settings_need_no_database_credentials() -> None:
    settings = LandingSettings.from_env({})

    assert settings.kafka == KafkaSettings.from_env({})
    assert settings.lake == LakeSettings.from_env({})
    assert settings.trigger_seconds == 10


def test_landing_trigger_interval_can_be_overridden() -> None:
    assert LandingSettings.from_env({"LANDING_TRIGGER_SECONDS": "30"}).trigger_seconds == 30


def test_landing_paths_use_one_folder_per_topic() -> None:
    settings = LandingSettings.from_env({})
    topic = "finance_db.operations.orders"

    assert settings.landing_path(topic) == "s3a://lake/landing/finance_db.operations.orders"
    assert settings.checkpoint_path(topic) == (
        "s3a://lake/checkpoints/landing/finance_db.operations.orders"
    )
