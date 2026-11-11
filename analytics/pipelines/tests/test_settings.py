import pytest

from common.settings import AnalyticsDbSettings, InvalidSettingError, MissingSettingsError
from pipelines.settings import (
    BronzeSettings,
    GoldRetentionSettings,
    GoldSettings,
    KafkaSettings,
    LakeSettings,
    LandingQualitySettings,
    LandingSettings,
    MaintenanceSettings,
    PipelineSettings,
    SilverSettings,
    StageExporterSettings,
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


def test_each_layer_has_a_namespace_in_the_catalog_and_a_checkpoint_folder() -> None:
    settings = LakeSettings.from_env(LAKE)

    assert settings.table("silver", "customers") == "catalog.silver.customers"
    assert settings.checkpoint_path("silver", "customers") == (
        "s3a://bucket/checkpoints/silver/customers"
    )


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


def test_silver_settings_need_only_the_topics_and_the_lake() -> None:
    settings = SilverSettings.from_env({"KAFKA_TOPICS": "topic-a, topic-b"} | LAKE)

    assert settings.topics == ("topic-a", "topic-b")
    assert settings.lake == LakeSettings.from_env(LAKE)
    assert settings.trigger_seconds == 10


def test_silver_trigger_interval_can_be_overridden() -> None:
    settings = SilverSettings.from_env(KAFKA | LAKE | {"SILVER_TRIGGER_SECONDS": "30"})

    assert settings.trigger_seconds == 30


@pytest.mark.parametrize("value", ["0", "-5", "ten", "1.5"])
def test_silver_trigger_interval_must_be_a_positive_integer(value: str) -> None:
    with pytest.raises(InvalidSettingError, match="SILVER_TRIGGER_SECONDS"):
        SilverSettings.from_env(KAFKA | LAKE | {"SILVER_TRIGGER_SECONDS": value})


def test_silver_settings_report_every_missing_variable_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        SilverSettings.from_env({})

    assert error.value.names == ("KAFKA_TOPICS", *LakeSettings.REQUIRED)


def test_silver_reads_the_bronze_tables_and_writes_its_own_namespace() -> None:
    settings = SilverSettings.from_env(KAFKA | LAKE)

    assert settings.source_table("customers") == BronzeSettings.from_env(KAFKA | LAKE).table(
        "customers"
    )
    assert settings.table("customers") == "catalog.silver.customers"
    assert settings.checkpoint_path("customers") == "s3a://bucket/checkpoints/silver/customers"


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


def test_gold_settings_read_the_lake_the_database_and_the_trigger() -> None:
    settings = GoldSettings.from_env(LAKE | DATABASE | {"GOLD_TRIGGER_SECONDS": "30"})

    assert settings.lake == LakeSettings.from_env(LAKE)
    assert settings.analytics_db == AnalyticsDbSettings.from_env(DATABASE)
    assert settings.trigger_seconds == 30


def test_gold_settings_trigger_every_ten_seconds_by_default() -> None:
    assert GoldSettings.from_env(LAKE | DATABASE).trigger_seconds == 10


def test_gold_settings_require_the_lake_and_the_database() -> None:
    with pytest.raises(MissingSettingsError) as error:
        GoldSettings.from_env({})

    assert error.value.names == (*LakeSettings.REQUIRED, *AnalyticsDbSettings.REQUIRED)


def test_gold_settings_reject_a_trigger_that_is_not_a_positive_integer() -> None:
    with pytest.raises(InvalidSettingError, match="GOLD_TRIGGER_SECONDS"):
        GoldSettings.from_env(LAKE | DATABASE | {"GOLD_TRIGGER_SECONDS": "0"})


def test_gold_reads_silver_tables_and_keeps_checkpoints_per_gold_table() -> None:
    settings = GoldSettings.from_env(LAKE | DATABASE)

    assert settings.source_table("customers") == "catalog.silver.customers"
    assert settings.checkpoint_path("dim_customer") == "s3a://bucket/checkpoints/gold/dim_customer"


MAINTENANCE = {"ICEBERG_CATALOG": "catalog", "LAKE_ROOT": "s3a://lake"}


def test_maintenance_defaults_cover_bronze_and_silver_with_a_week_of_snapshots() -> None:
    settings = MaintenanceSettings.from_env(MAINTENANCE)

    assert settings.catalog == "catalog"
    assert settings.namespaces == ("bronze", "silver")
    assert settings.snapshot_retention_hours == 168
    assert settings.retain_last == 10
    assert settings.orphan_age_hours == 72


def test_the_silver_and_gold_streams_are_the_default_downstream_of_the_tables_they_read() -> None:
    settings = MaintenanceSettings.from_env(MAINTENANCE | {"CHECKPOINT_ROOT": "s3a://cp/"})

    assert settings.downstream == {
        **{
            f"bronze.{entity}": (f"s3a://cp/silver/{entity}",)
            for entity in ("customers", "order_items", "orders", "products")
        },
        "silver.customers": ("s3a://cp/gold/dim_customer",),
        "silver.products": ("s3a://cp/gold/dim_product",),
        "silver.orders": ("s3a://cp/gold/fact_order_history",),
        "silver.order_items": ("s3a://cp/gold/fact_order_item_history",),
    }
    assert settings.checkpoints("bronze.unknown") == ()


def test_maintenance_settings_can_be_overridden() -> None:
    settings = MaintenanceSettings.from_env(
        MAINTENANCE
        | {
            "MAINTENANCE_NAMESPACES": "bronze, test",
            "MAINTENANCE_SNAPSHOT_RETENTION_HOURS": "24",
            "MAINTENANCE_RETAIN_LAST": "3",
            "MAINTENANCE_ORPHAN_AGE_HOURS": "48",
            "MAINTENANCE_DOWNSTREAM_CHECKPOINTS": "bronze.orders = s3a://a | s3a://b, silver.x=s3a://c",
        }
    )

    assert settings.namespaces == ("bronze", "test")
    assert settings.snapshot_retention_hours == 24
    assert settings.retain_last == 3
    assert settings.orphan_age_hours == 48
    assert settings.downstream == {
        "bronze.orders": ("s3a://a", "s3a://b"),
        "silver.x": ("s3a://c",),
    }


def test_maintenance_settings_require_the_lake() -> None:
    with pytest.raises(MissingSettingsError) as error:
        MaintenanceSettings.from_env({})

    assert error.value.names == LakeSettings.REQUIRED


@pytest.mark.parametrize(
    "name",
    [
        "MAINTENANCE_SNAPSHOT_RETENTION_HOURS",
        "MAINTENANCE_RETAIN_LAST",
        "MAINTENANCE_ORPHAN_AGE_HOURS",
    ],
)
def test_maintenance_numbers_must_be_positive_integers(name: str) -> None:
    with pytest.raises(InvalidSettingError, match=name):
        MaintenanceSettings.from_env(MAINTENANCE | {name: "0"})


def test_orphan_files_must_be_at_least_a_day_old() -> None:
    with pytest.raises(InvalidSettingError, match="at least 24"):
        MaintenanceSettings.from_env(MAINTENANCE | {"MAINTENANCE_ORPHAN_AGE_HOURS": "23"})


def test_maintenance_namespaces_must_not_be_empty() -> None:
    with pytest.raises(InvalidSettingError, match="MAINTENANCE_NAMESPACES"):
        MaintenanceSettings.from_env(MAINTENANCE | {"MAINTENANCE_NAMESPACES": ","})


@pytest.mark.parametrize("value", ["bronze.orders", "orders=s3a://a", "bronze.orders= | "])
def test_downstream_checkpoints_must_name_a_table_and_a_path(value: str) -> None:
    with pytest.raises(InvalidSettingError, match="MAINTENANCE_DOWNSTREAM_CHECKPOINTS"):
        MaintenanceSettings.from_env(MAINTENANCE | {"MAINTENANCE_DOWNSTREAM_CHECKPOINTS": value})


def test_gold_retention_keeps_three_years_and_two_months_ahead_by_default() -> None:
    settings = GoldRetentionSettings.from_env(DATABASE)

    assert settings.analytics_db == AnalyticsDbSettings.from_env(DATABASE)
    assert settings.keep_months == 36
    assert settings.months_ahead == 2


def test_gold_retention_window_and_months_ahead_can_be_overridden() -> None:
    settings = GoldRetentionSettings.from_env(
        DATABASE | {"GOLD_RETENTION_MONTHS": "24", "GOLD_PARTITION_MONTHS_AHEAD": "6"}
    )

    assert settings.keep_months == 24
    assert settings.months_ahead == 6


@pytest.mark.parametrize("name", ["GOLD_RETENTION_MONTHS", "GOLD_PARTITION_MONTHS_AHEAD"])
def test_gold_retention_months_must_be_positive_integers(name: str) -> None:
    with pytest.raises(InvalidSettingError, match=name):
        GoldRetentionSettings.from_env(DATABASE | {name: "0"})


def test_gold_retention_needs_only_the_database() -> None:
    with pytest.raises(MissingSettingsError) as error:
        GoldRetentionSettings.from_env({})

    assert error.value.names == AnalyticsDbSettings.REQUIRED


STAGE_EXPORTER = {"HISTORY_SERVER_URL": "http://history:18080", "STAGE_EXPORTER_PORT": "9101"}


def test_the_stage_exporter_polls_the_history_server_every_thirty_seconds_by_default() -> None:
    assert StageExporterSettings.from_env(STAGE_EXPORTER) == StageExporterSettings(
        history_server_url="http://history:18080", port=9101, poll_seconds=30
    )


def test_the_stage_exporter_poll_interval_can_be_overridden() -> None:
    settings = StageExporterSettings.from_env(STAGE_EXPORTER | {"STAGE_EXPORTER_POLL_SECONDS": "5"})

    assert settings.poll_seconds == 5


def test_the_stage_exporter_needs_the_history_server_and_its_own_port() -> None:
    with pytest.raises(MissingSettingsError) as error:
        StageExporterSettings.from_env({})

    assert error.value.names == ("HISTORY_SERVER_URL", "STAGE_EXPORTER_PORT")


@pytest.mark.parametrize("name", ["STAGE_EXPORTER_PORT", "STAGE_EXPORTER_POLL_SECONDS"])
def test_the_stage_exporter_port_and_interval_must_be_positive_integers(name: str) -> None:
    with pytest.raises(InvalidSettingError, match=name):
        StageExporterSettings.from_env(STAGE_EXPORTER | {name: "0"})
