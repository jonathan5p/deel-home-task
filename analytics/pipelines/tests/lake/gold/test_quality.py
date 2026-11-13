import pytest

pytest.importorskip("pyspark")

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.pipelines.tests.lake.gold.model.model_samples import at
from analytics.pipelines.tests.lake.gold.quality_setup import (
    SNAPSHOT,
    FakeGold,
    GoldTables,
    Synced,
    by_check,
    samples,
)
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.quality import (
    COPY_MATCHES_ICEBERG,
    NO_OVERLAPPING_RANGES,
    ONE_CURRENT_VERSION,
    REPORTS_MATCH_RECOUNT,
    RESOLVED_LOOKUPS,
    compare,
    plain,
    sample,
)

COPY = COPY_MATCHES_ICEBERG.name


CHECKED = {
    COPY: len(STORED),
    ONE_CURRENT_VERSION.name: 6,
    NO_OVERLAPPING_RANGES.name: 4,
    RESOLVED_LOOKUPS.name: 4,
    REPORTS_MATCH_RECOUNT.name: 5,
}


def test_compare_casts_postgres_to_the_lake_types(spark: SparkSession) -> None:
    lake = spark.createDataFrame([(1, "1.5")], "id long, price string").select(
        "id", F.col("price").cast("decimal(38,10)").alias("price")
    )
    postgres = spark.createDataFrame([(1, "1.500")], "id long, price string").select(
        "id", F.col("price").cast("decimal(38,18)").alias("price")
    )

    found = compare(lake, postgres)

    assert (found.lake_rows, found.lake_only, found.postgres_only, found.passed) == (1, 0, 0, True)


def test_report_values_are_plain_json(spark: SparkSession) -> None:
    rows = spark.createDataFrame(
        [(2, date(2026, 11, 1), Decimal("1.50")), (1, None, None)],
        "id int, day date, price decimal(10,2)",
    )

    assert sample(rows, ["id", "day", "price"], 5) == (
        {"id": 1, "day": None, "price": None},
        {"id": 2, "day": "2026-11-01", "price": "1.50"},
    )
    assert plain(datetime(2026, 11, 1, 10, 0)) == "2026-11-01T10:00:00"


def test_a_copy_that_matches_iceberg_and_keeps_the_invariants_passes_every_check(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    results = FakeGold.mirrored(spark, gold_tables).results()

    assert [result for result in results if not result.passed] == []
    assert {check: len(by_check(results, check)) for check in CHECKED} == CHECKED
    assert len(results) == sum(CHECKED.values())
    assert results[0].subject == "lake.gold_internal.dim_order_status"


def test_a_row_missing_from_the_copy_fails_its_table_naming_its_key(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    relation = STORED["current_orders"].relation
    gold.postgres[relation] = gold.postgres[relation].where("order_id <> 71")

    result = by_check(gold.results(), COPY)["current_orders"]

    assert not result.passed
    assert result.metrics == {
        "lake_rows": 3,
        "postgres_rows": 2,
        "lake_only": 1,
        "postgres_only": 0,
    }
    assert samples(result, "lake_only") == ({"order_id": 71},)
    assert result.details["snapshot_id"] == SNAPSHOT


def test_history_before_the_retention_window_is_left_out_of_the_comparison(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    history = gold.lake["fact_order_history"][SNAPSHOT]
    old = history.where("_commit_sequence = 30").withColumn(
        "_changed_at", F.lit(datetime(2020, 1, 1))
    )
    gold.lake["fact_order_history"][SNAPSHOT] = history.unionByName(old)

    result = by_check(gold.results(), COPY)["fact_order_history"]

    assert result.passed
    assert result.metrics["lake_rows"] == result.metrics["postgres_rows"] == history.count()
    assert result.details["history_from"] == "2023-11-01T00:00:00"


def test_a_table_never_copied_is_not_checked_nor_the_invariants_that_read_it(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.sync_states = [{name: SNAPSHOT for name in STORED if name != "dim_product"}]

    results = gold.results()

    copy = by_check(results, COPY)["dim_product"]
    assert not copy.passed
    assert copy.metrics == dict.fromkeys(copy.metrics)
    assert "records no copy" in str(copy.details["not_checked"])
    lookups = by_check(results, RESOLVED_LOOKUPS.name)["fact_order_item_history"]
    assert not lookups.passed
    assert lookups.details == {"not_checked": "no readable copied snapshot of dim_product"}
    assert by_check(results, RESOLVED_LOOKUPS.name)["fact_order_history"].passed


def test_a_copy_of_a_snapshot_iceberg_no_longer_has_fails(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.sync_states = [dict.fromkeys(STORED, SNAPSHOT) | {"dim_customer": SNAPSHOT - 1}]

    result = by_check(gold.results(), COPY)["dim_customer"]

    assert not result.passed
    assert result.details == {
        "not_checked": f"its Iceberg table no longer has snapshot {SNAPSHOT - 1}"
    }


def test_a_table_copied_without_a_snapshot_is_compared_with_an_empty_table(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    relation = STORED["top_customers_by_pending_orders"].relation
    gold.postgres[relation] = gold.postgres[relation].limit(0)
    gold.sync_states = [dict.fromkeys(STORED, SNAPSHOT) | {"top_customers_by_pending_orders": None}]

    result = by_check(gold.results(), COPY)["top_customers_by_pending_orders"]

    assert result.passed
    assert result.details["snapshot_id"] is None


def test_a_table_copied_into_while_read_is_read_again_at_its_new_snapshot(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.lake["current_orders"][SNAPSHOT + 1] = gold.lake["current_orders"][SNAPSHOT]
    first: Synced = dict.fromkeys(STORED, SNAPSHOT)
    gold.sync_states = [first, first | {"current_orders": SNAPSHOT + 1}]

    result = by_check(gold.results(), COPY)["current_orders"]

    assert result.passed
    assert result.details["snapshot_id"] == SNAPSHOT + 1
    assert gold.reads.count(STORED["current_orders"].relation) == 2


def test_a_table_copied_into_during_every_read_is_not_checked(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.sync_states = [dict.fromkeys(STORED, SNAPSHOT) | {"current_orders": n} for n in range(4)]

    result = by_check(gold.results(), COPY)["current_orders"]

    assert not result.passed
    assert "during each of 3 reads" in str(result.details["not_checked"])


def test_two_current_versions_of_a_customer_fail(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.change("dim_customer", lambda rows: rows.withColumn("_is_current", F.lit(True)))

    result = by_check(gold.results(), ONE_CURRENT_VERSION.name)["dim_customer"]

    assert not result.passed
    assert result.metrics == {"rows": 2, "violations": 1}
    assert samples(result, "violating") == ({"customer_id": 1},)


def test_two_current_state_rows_of_an_order_fail(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.change("current_orders", lambda rows: rows.unionByName(rows.where("order_id = 70")))

    result = by_check(gold.results(), ONE_CURRENT_VERSION.name)["current_orders"]

    assert not result.passed
    assert samples(result, "violating") == ({"order_id": 70},)


def test_a_version_ending_after_the_next_one_starts_fails(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)

    def overlap(rows: DataFrame) -> DataFrame:
        late_end = F.when(F.col("_commit_sequence") == 10, F.lit(at(25)))
        return rows.withColumn("_valid_to", late_end.otherwise(F.col("_valid_to")))

    gold.change("dim_customer", overlap)

    result = by_check(gold.results(), NO_OVERLAPPING_RANGES.name)["dim_customer"]

    assert not result.passed
    assert samples(result, "violating") == ({"customer_id": 1, "_commit_sequence": 10},)


def test_a_status_key_no_status_has_fails_and_a_null_date_key_does_not(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)

    def unknown_status(rows: DataFrame) -> DataFrame:
        status = F.when(F.col("order_id") == 71, F.lit(99)).otherwise(F.col("status_key"))
        return rows.withColumn("status_key", status).withColumn(
            "delivery_date_key", F.lit(None).cast("int")
        )

    gold.change("current_orders", unknown_status)

    result = by_check(gold.results(), RESOLVED_LOOKUPS.name)["current_orders"]

    assert not result.passed
    assert samples(result, "violating") == ({"order_id": 71, "_commit_sequence": 31},)


def test_an_item_naming_another_product_version_than_the_model_resolves_fails(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)

    def other_version(rows: DataFrame) -> DataFrame:
        key = F.when(F.col("order_item_id") == 80, F.lit(12345)).otherwise(F.col("product_key"))
        return rows.withColumn("product_key", key.cast("long"))

    gold.change("fact_order_item_history", other_version)

    result = by_check(gold.results(), RESOLVED_LOOKUPS.name)["fact_order_item_history"]

    assert not result.passed
    assert samples(result, "violating") == ({"order_item_id": 80, "_commit_sequence": 33},)


def test_a_report_total_that_differs_from_the_recount_fails_with_both_rows(
    spark: SparkSession, gold_tables: GoldTables
) -> None:
    gold = FakeGold.mirrored(spark, gold_tables)
    gold.change(
        "pending_items_by_product",
        lambda rows: rows.withColumn("pending_quantity", F.col("pending_quantity") + 1),
    )

    result = by_check(gold.results(), REPORTS_MATCH_RECOUNT.name)["pending_items_by_product"]

    assert not result.passed
    assert result.metrics == {"rows": 1, "violations": 2}
    assert [row["side"] for row in samples(result, "violating")] == ["recounted", "stored"]
