import shutil
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.errors import SparkRuntimeException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from pipelines.jobs.landing import start_landing
from pipelines.quality.checks import CheckResult, all_passed
from pipelines.quality.landing import (
    LandingSchemaError,
    read_folder,
    read_kafka,
    read_landing,
    reconcile,
)
from pipelines.schemas import KAFKA_RECORD_SCHEMA
from pipelines.settings import KafkaSettings, LandingSettings

PUBLISHED = datetime(2026, 11, 8, 14, 21)  # Spark returns naive timestamps in UTC
TOPIC = "topic-a"
TOPICS = (TOPIC,)


def record(offset: int, *, value: bytes | None = b"v", topic: str = TOPIC) -> Row:
    return Row(
        key=f"k{offset}".encode(),
        value=value,
        topic=topic,
        partition=0,
        offset=offset,
        timestamp=PUBLISHED,
        timestampType=0,
    )


def frame(spark: SparkSession, rows: list[Row]) -> DataFrame:
    return spark.createDataFrame(rows, KAFKA_RECORD_SCHEMA)


def offsets(spark: SparkSession, numbers: list[int] | range) -> DataFrame:
    return frame(spark, [record(number) for number in numbers])


def by_check(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {result.check: result for result in results}


def landing_settings(tmp_path: Path, topics: str = TOPIC) -> LandingSettings:
    return LandingSettings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPICS": topics,
            "LAKE_ROOT": f"file://{tmp_path}/lake",
            "ICEBERG_CATALOG": "catalog",
        }
    )


def local(path: str) -> Path:
    return Path(path.removeprefix("file://"))


def land_with_file_sink(
    spark: SparkSession,
    settings: LandingSettings,
    source: Path,
    *,
    files_per_batch: int | None = None,
) -> None:
    reader = spark.readStream.schema(KAFKA_RECORD_SCHEMA)
    if files_per_batch is not None:
        reader = reader.option("maxFilesPerTrigger", files_per_batch)
    query = start_landing(reader.parquet(str(source)), TOPIC, settings, available_now=True)
    query.awaitTermination()


def stage(spark: SparkSession, source: Path, rows: list[Row]) -> None:
    frame(spark, rows).coalesce(1).write.mode("append").parquet(str(source))


class TestCompleteLanding:
    @pytest.fixture
    def results(self, spark: SparkSession) -> dict[str, CheckResult]:
        landed, kafka = offsets(spark, range(4)), offsets(spark, range(6))
        return by_check(reconcile(landed, kafka, topics=TOPICS))

    def test_runs_every_partition_check(self, results: dict[str, CheckResult]) -> None:
        assert set(results) == {
            "partition_landed",
            "unique_offsets",
            "contiguous_offsets",
            "starts_at_kafka_min_offset",
            "same_bytes_as_kafka",
        }

    def test_every_check_passes(self, results: dict[str, CheckResult]) -> None:
        assert all(result.passed for result in results.values())

    def test_every_result_carries_its_check_description(
        self, results: dict[str, CheckResult]
    ) -> None:
        assert all(result.description for result in results.values())

    def test_partition_landed_reports_the_offsets_and_the_lag(
        self, results: dict[str, CheckResult]
    ) -> None:
        assert results["partition_landed"].subject == "topic-a[0]"
        assert results["partition_landed"].metrics == {
            "rows": 4,
            "min_offset": 0,
            "max_offset": 3,
            "tombstones": 0,
            "kafka_min_offset": 0,
            "kafka_max_offset": 5,
            "lag": 2,
        }

    def test_contiguous_offsets_reports_the_compared_range(
        self, results: dict[str, CheckResult]
    ) -> None:
        assert results["contiguous_offsets"].metrics == {
            "from_offset": 0,
            "to_offset": 3,
            "missing_offsets": 0,
        }


class TestUniqueOffsets:
    def test_a_duplicated_offset_fails(self, spark: SparkSession) -> None:
        results = by_check(
            reconcile(offsets(spark, [0, 1, 1, 2]), offsets(spark, range(3)), topics=TOPICS)
        )

        assert not results["unique_offsets"].passed
        assert results["unique_offsets"].metrics == {"rows": 4, "distinct_offsets": 3}


class TestContiguousOffsets:
    def test_an_offset_kafka_holds_but_did_not_land_fails(self, spark: SparkSession) -> None:
        results = by_check(
            reconcile(offsets(spark, [0, 1, 3]), offsets(spark, range(4)), topics=TOPICS)
        )

        assert not results["contiguous_offsets"].passed
        assert results["contiguous_offsets"].metrics == {
            "from_offset": 0,
            "to_offset": 3,
            "missing_offsets": 1,
        }

    def test_an_offset_kafka_does_not_hold_is_not_a_gap(self, spark: SparkSession) -> None:
        kafka = offsets(spark, [0, 1, 3, 4])  # offset 2 is a transaction marker or was compacted
        landed = offsets(spark, [0, 1, 3, 4])

        results = by_check(reconcile(landed, kafka, topics=TOPICS))

        assert all(result.passed for result in results.values())
        assert results["contiguous_offsets"].metrics["missing_offsets"] == 0

    def test_kafka_offsets_after_the_last_landed_offset_are_lag_not_gaps(
        self, spark: SparkSession
    ) -> None:
        results = by_check(
            reconcile(offsets(spark, range(3)), offsets(spark, range(10)), topics=TOPICS)
        )

        assert results["contiguous_offsets"].passed
        assert results["partition_landed"].metrics["lag"] == 7


class TestStartsAtKafkaMinOffset:
    def test_landing_that_starts_after_the_kafka_min_offset_fails(
        self, spark: SparkSession
    ) -> None:
        results = by_check(
            reconcile(offsets(spark, [2, 3]), offsets(spark, range(4)), topics=TOPICS)
        )

        assert not results["starts_at_kafka_min_offset"].passed
        assert results["starts_at_kafka_min_offset"].metrics == {
            "min_offset": 2,
            "kafka_min_offset": 0,
        }


class TestSameBytesAsKafka:
    def test_a_changed_value_fails(self, spark: SparkSession) -> None:
        kafka = offsets(spark, range(3))
        landed = frame(spark, [record(0), record(1, value=b"changed"), record(2)])

        results = by_check(reconcile(landed, kafka, topics=TOPICS))

        assert not results["same_bytes_as_kafka"].passed
        assert results["same_bytes_as_kafka"].metrics == {
            "compared": 3,
            "different": 1,
            "missing_in_kafka": 0,
        }

    def test_a_landed_offset_that_kafka_retains_but_lacks_fails(self, spark: SparkSession) -> None:
        results = by_check(
            reconcile(offsets(spark, range(4)), offsets(spark, [0, 1, 3]), topics=TOPICS)
        )

        assert not results["same_bytes_as_kafka"].passed
        assert results["same_bytes_as_kafka"].metrics == {
            "compared": 3,
            "different": 0,
            "missing_in_kafka": 1,
        }

    def test_landed_offsets_that_kafka_no_longer_retains_are_not_missing(
        self, spark: SparkSession
    ) -> None:
        results = by_check(
            reconcile(offsets(spark, range(6)), offsets(spark, [4, 5]), topics=TOPICS)
        )

        assert all(result.passed for result in results.values())
        assert results["same_bytes_as_kafka"].metrics == {
            "compared": 2,
            "different": 0,
            "missing_in_kafka": 0,
        }

    def test_a_partition_whose_kafka_records_all_expired_passes(self, spark: SparkSession) -> None:
        results = by_check(reconcile(offsets(spark, range(3)), frame(spark, []), topics=TOPICS))

        assert all(result.passed for result in results.values())
        assert "starts_at_kafka_min_offset" not in results
        assert results["same_bytes_as_kafka"].metrics == {
            "compared": 0,
            "different": 0,
            "missing_in_kafka": 0,
        }
        assert results["partition_landed"].metrics["lag"] is None

    def test_tombstones_are_counted_and_compared(self, spark: SparkSession) -> None:
        rows = [record(0), record(1, value=None)]

        results = by_check(reconcile(frame(spark, rows), frame(spark, rows), topics=TOPICS))

        assert results["partition_landed"].metrics["tombstones"] == 1
        assert results["same_bytes_as_kafka"].passed

    def test_records_match_on_topic_partition_and_offset(self, spark: SparkSession) -> None:
        landed = frame(spark, [record(0), record(0, topic="topic-b")])
        kafka = frame(spark, [record(0), record(0, topic="topic-b", value=b"other")])

        results = reconcile(landed, kafka, topics=("topic-a", "topic-b"))

        failed = [(result.check, result.subject) for result in results if not result.passed]
        assert failed == [("same_bytes_as_kafka", "topic-b[0]")]


class TestPresence:
    def test_a_kafka_partition_that_never_landed_fails(self, spark: SparkSession) -> None:
        kafka = frame(spark, [record(0, topic="topic-b")])

        results = reconcile(frame(spark, []), kafka, topics=("topic-b",))

        assert [(result.check, result.subject, result.passed) for result in results] == [
            ("partition_landed", "topic-b[0]", False)
        ]
        assert results[0].metrics == {"rows": 0, "kafka_min_offset": 0, "kafka_max_offset": 0}

    def test_a_configured_topic_with_no_records_anywhere_fails(self, spark: SparkSession) -> None:
        results = reconcile(
            offsets(spark, range(2)), offsets(spark, range(2)), topics=(TOPIC, "topic-b")
        )

        missing = [result for result in results if result.check == "topic_found"]
        assert [(result.subject, result.passed) for result in missing] == [("topic-b", False)]
        assert not all_passed(results)

    def test_a_run_with_nothing_to_reconcile_does_not_pass(self, spark: SparkSession) -> None:
        results = reconcile(frame(spark, []), frame(spark, []), topics=TOPICS)

        assert [(result.check, result.subject) for result in results] == [("topic_found", TOPIC)]
        assert not all_passed(results)


class TestWindow:
    def test_compares_only_the_latest_landed_offsets(self, spark: SparkSession) -> None:
        landed = offsets(spark, [0, 2, 3, 4, 5])

        results = by_check(
            reconcile(landed, offsets(spark, range(6)), topics=TOPICS, last_offsets=3)
        )

        assert all(result.passed for result in results.values())
        assert results["partition_landed"].metrics["min_offset"] == 3
        assert results["contiguous_offsets"].metrics == {
            "from_offset": 3,
            "to_offset": 5,
            "missing_offsets": 0,
        }
        assert "starts_at_kafka_min_offset" not in results

    def test_a_gap_at_the_start_of_the_window_fails(self, spark: SparkSession) -> None:
        landed = offsets(spark, [*range(6), 9])

        results = by_check(
            reconcile(landed, offsets(spark, range(10)), topics=TOPICS, last_offsets=3)
        )

        assert not results["contiguous_offsets"].passed
        assert results["contiguous_offsets"].metrics == {
            "from_offset": 7,
            "to_offset": 9,
            "missing_offsets": 2,
        }

    def test_a_window_larger_than_the_partition_starts_at_offset_zero(
        self, spark: SparkSession
    ) -> None:
        results = by_check(
            reconcile(
                offsets(spark, range(3)), offsets(spark, range(3)), topics=TOPICS, last_offsets=10
            )
        )

        assert results["contiguous_offsets"].metrics == {
            "from_offset": 0,
            "to_offset": 2,
            "missing_offsets": 0,
        }


class TestReading:
    def test_read_kafka_reads_the_kafka_record_columns(self, spark: SparkSession) -> None:
        kafka = KafkaSettings(bootstrap_servers="broker:9092", topics=("topic-a",))

        records = read_kafka(spark, kafka)

        assert records.columns == KAFKA_RECORD_SCHEMA.fieldNames()

    def test_a_missing_landing_folder_reads_as_no_records(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        landed = read_landing(spark, landing_settings(tmp_path))

        assert landed.count() == 0
        assert landed.schema == KAFKA_RECORD_SCHEMA

    def test_a_folder_without_committed_files_reads_as_no_records(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        (tmp_path / "landing" / "_spark_metadata").mkdir(parents=True)

        landed = read_folder(spark, f"file://{tmp_path}/landing")

        assert landed.count() == 0
        assert landed.schema == KAFKA_RECORD_SCHEMA

    def test_a_folder_of_other_records_raises_a_schema_error(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        path = f"file://{tmp_path}/landing"
        spark.createDataFrame([(1, "a")], "id int, name string").write.format("avro").save(path)

        with pytest.raises(LandingSchemaError, match="id"):
            read_folder(spark, path)

    def test_a_column_with_another_type_raises_a_schema_error(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        path = f"file://{tmp_path}/landing"
        as_int = F.col("offset").cast("int")
        offsets(spark, range(2)).withColumn("offset", as_int).write.format("avro").save(path)

        with pytest.raises(LandingSchemaError, match="offset"):
            read_folder(spark, path)

    def test_other_read_errors_propagate(self, spark: SparkSession, tmp_path: Path) -> None:
        for folder in ("a=1", "b=2"):
            offsets(spark, range(1)).write.format("avro").save(f"{tmp_path}/landing/{folder}")

        with pytest.raises(SparkRuntimeException):
            read_folder(spark, f"file://{tmp_path}/landing")

    def test_read_landing_reads_every_topic_folder(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        settings = landing_settings(tmp_path, "topic-a,topic-b")
        for topic in ("topic-a", "topic-b"):
            rows = [record(0, topic=topic), record(1, topic=topic)]
            frame(spark, rows).write.format("avro").save(settings.landing_path(topic))

        landed = read_landing(spark, settings)

        assert sorted((row.topic, row.offset) for row in landed.collect()) == [
            ("topic-a", 0),
            ("topic-a", 1),
            ("topic-b", 0),
            ("topic-b", 1),
        ]


class TestReplay:
    def test_files_the_sink_did_not_commit_are_ignored(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        settings = landing_settings(tmp_path)
        rows = [record(offset) for offset in range(4)]
        stage(spark, tmp_path / "source", rows)
        land_with_file_sink(spark, settings, tmp_path / "source")
        folder = local(settings.landing_path(TOPIC))
        committed = next(folder.glob("*.avro"))
        shutil.copy(committed, folder / f"uncommitted-{committed.name}")

        results = reconcile(read_landing(spark, settings), frame(spark, rows), topics=TOPICS)

        assert all_passed(results)
        assert by_check(results)["partition_landed"].metrics["rows"] == 4

    def test_offsets_landed_again_after_a_checkpoint_reset_fail_uniqueness(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        settings = landing_settings(tmp_path)
        source = tmp_path / "source"
        stage(spark, source, [record(offset) for offset in range(3)])
        stage(spark, source, [record(offset) for offset in range(3, 6)])
        land_with_file_sink(spark, settings, source)
        shutil.rmtree(local(settings.checkpoint_path(TOPIC)))
        # The sink skips batch ids it already committed, so only a later batch lands again.
        land_with_file_sink(spark, settings, source, files_per_batch=1)

        results = by_check(
            reconcile(read_landing(spark, settings), offsets(spark, range(6)), topics=TOPICS)
        )

        assert not results["unique_offsets"].passed
        assert results["unique_offsets"].metrics == {"rows": 9, "distinct_offsets": 6}
