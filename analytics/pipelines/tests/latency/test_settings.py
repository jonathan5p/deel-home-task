import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.latency_setup import ENV
from common.settings import InvalidSettingError, MissingSettingsError
from pipelines.latency.layers import Layer
from pipelines.latency.settings import (
    LakeReadSettings,
    LatencySettings,
    ProbeOptions,
    SourceDbSettings,
    parse_max_latency,
    parse_options,
)

ENTITIES = ("customers", "products", "orders")


def test_latency_settings_read_the_lake_its_readers_and_both_databases() -> None:
    settings = LatencySettings.from_env(ENV)

    assert settings.topics == ("prefix.operations.customers", "prefix.operations.orders")
    assert settings.lake.root == "s3a://bucket"
    assert settings.lake.catalog == "catalog"
    assert settings.lake_read.catalog_uri == "http://catalog:8181"
    assert settings.source_db.conninfo() == "postgresql://source:5432/source"
    assert settings.analytics_db.user == "owner"
    assert settings.extension_directory == "/opt/extensions"


def test_every_trigger_interval_defaults_to_the_job_default_and_can_be_set() -> None:
    settings = LatencySettings.from_env(ENV | {"GOLD_TRIGGER_SECONDS": "30"})

    assert settings.trigger_seconds == {
        Layer.LANDING: 10,
        Layer.BRONZE: 10,
        Layer.SILVER: 10,
        Layer.GOLD: 30,
    }
    assert settings.longest_trigger_seconds == 30


def test_latency_settings_report_every_missing_variable_at_once() -> None:
    with pytest.raises(MissingSettingsError) as error:
        LatencySettings.from_env({"KAFKA_TOPICS": "topic-a"})

    assert "SOURCE_DB_PASSWORD" in error.value.names
    assert "LAKE_CATALOG_URI" in error.value.names
    assert "ANALYTICS_DB_URL" in error.value.names
    assert "DUCKDB_EXTENSION_DIRECTORY" in error.value.names


def test_a_trigger_interval_must_be_a_positive_integer() -> None:
    with pytest.raises(InvalidSettingError, match="SILVER_TRIGGER_SECONDS"):
        LatencySettings.from_env(ENV | {"SILVER_TRIGGER_SECONDS": "0"})


def test_the_object_storage_endpoint_gives_the_host_and_whether_to_use_tls() -> None:
    plain = LakeReadSettings.from_env(ENV)
    secure = LakeReadSettings.from_env(ENV | {"LAKE_S3_ENDPOINT": "https://storage.example/"})

    assert (plain.s3_host, plain.s3_uses_ssl) == ("storage:8333", False)
    assert (secure.s3_host, secure.s3_uses_ssl) == ("storage.example", True)


def test_the_object_storage_endpoint_must_be_an_http_url() -> None:
    with pytest.raises(InvalidSettingError, match="LAKE_S3_ENDPOINT"):
        LakeReadSettings.from_env(ENV | {"LAKE_S3_ENDPOINT": "storage:8333"})


def test_passwords_stay_out_of_the_settings_repr() -> None:
    assert "p@ss" not in repr(SourceDbSettings.from_env(ENV))
    assert "secret" not in repr(LakeReadSettings.from_env(ENV))


def test_the_options_default_to_every_entity_and_five_probes_without_thresholds() -> None:
    assert parse_options([], ENTITIES) == ProbeOptions(
        entities=ENTITIES, probes=5, poll_seconds=1.0, timeout_seconds=300.0
    )


def test_the_options_read_every_flag_and_keep_the_entities_in_their_known_order() -> None:
    options = parse_options(
        [
            "--entities",
            "orders, customers",
            "--probes",
            "3",
            "--poll-seconds",
            "0.5",
            "--timeout-seconds",
            "60",
            "--max-latency",
            "gold=60,silver=40.5",
            "--keys",
            "orders=12",
        ],
        ENTITIES,
    )

    assert options == ProbeOptions(
        entities=("customers", "orders"),
        probes=3,
        poll_seconds=0.5,
        timeout_seconds=60.0,
        max_latency={Layer.GOLD: 60.0, Layer.SILVER: 40.5},
        keys={"orders": 12},
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--entities", "payments"],
        ["--entities", " , "],
        ["--probes", "0"],
        ["--probes", "two"],
        ["--poll-seconds", "-1"],
        ["--timeout-seconds", "inf"],
        ["--max-latency", "lake=10"],
        ["--max-latency", "gold"],
        ["--max-latency", "gold=0"],
        ["--keys", "payments=1"],
        ["--keys", "orders=first"],
    ],
)
def test_invalid_options_exit_with_the_usage_error_code(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        parse_options(argv, ENTITIES)

    assert error.value.code == 2


def test_max_latency_maps_each_layer_to_its_limit_in_seconds() -> None:
    assert parse_max_latency(" gold = 60 , landing=5") == {Layer.GOLD: 60.0, Layer.LANDING: 5.0}
