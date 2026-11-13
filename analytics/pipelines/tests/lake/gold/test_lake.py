import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.gold.writer_setup import SEEDS, seeds
from pipelines.lake.gold.dimensions import STORED
from pipelines.lake.gold.lake import IcebergGold, described_schema

CURRENT = STORED["current_order_items"]
HISTORY = STORED["fact_order_item_history"]


@pytest.fixture
def lake(spark: SparkSession, iceberg_catalog: str) -> IcebergGold:
    gold = IcebergGold(
        spark, STORED, {name: f"{iceberg_catalog}.gold.internal_{name}" for name in STORED}
    )
    gold.ensure()
    return gold


def rows_of(lake: IcebergGold, name: str) -> list[tuple[object, ...]]:
    return sorted((tuple(row) for row in lake.read(name).collect()), key=str)


def properties(spark: SparkSession, relation: str) -> dict[str, str]:
    return {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {relation}").collect()}


def test_every_stored_table_is_created_empty_and_described_with_one_snapshot(
    spark: SparkSession, lake: IcebergGold
) -> None:
    for name, table in STORED.items():
        relation = lake.relation[name]
        assert spark.table(relation).schema == described_schema(table.table)
        assert spark.table(relation).isEmpty()
        assert lake.snapshot(name) is not None


def test_the_tables_are_format_2_and_merge_on_read(spark: SparkSession, lake: IcebergGold) -> None:
    stored = properties(spark, lake.relation["dim_customer"])

    assert stored["format-version"] == "2"
    assert stored["write.merge.mode"] == "merge-on-read"
    assert stored["write.update.mode"] == "merge-on-read"
    assert stored["write.delete.mode"] == "merge-on-read"


def test_a_history_table_is_bucketed_by_its_key_and_sorted_by_change(
    spark: SparkSession, lake: IcebergGold
) -> None:
    ((ddl,),) = spark.sql(f"SHOW CREATE TABLE {lake.relation[HISTORY.name]}").collect()

    assert "PARTITIONED BY (bucket(16, order_item_id))" in ddl
    assert "'sort-order' = 'order_item_id ASC NULLS FIRST, _commit_sequence ASC NULLS FIRST'" in (
        ddl
    )


def test_ensuring_again_updates_only_the_descriptions(
    spark: SparkSession, lake: IcebergGold
) -> None:
    relation = lake.relation["dim_order_status"]
    spark.sql(f"ALTER TABLE {relation} ALTER COLUMN status_code COMMENT 'old'")

    updates = lake.ensure()

    assert updates == [
        f"ALTER TABLE {relation} ALTER COLUMN `status_code`"
        " COMMENT 'Status as written in the source, or UNKNOWN'"
    ]


def test_a_merge_updates_matched_rows_and_inserts_new_ones(
    spark: SparkSession, lake: IcebergGold
) -> None:
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True), (2, "DONE", True)))

    lake.write(SEEDS, seeds(spark, (2, "DONE", False), (3, "NEW", True)))

    assert rows_of(lake, "dim_order_status") == [
        (1, "PENDING", True),
        (2, "DONE", False),
        (3, "NEW", True),
    ]


def test_a_current_row_is_only_replaced_by_a_newer_change(
    spark: SparkSession, lake: IcebergGold
) -> None:
    schema = lake.read(CURRENT.name).schema
    stored = [(1, 10, 3, 2, 50, None, False), (2, 20, 3, 2, 50, None, False)]
    lake.write(CURRENT, spark.createDataFrame(stored, schema))

    incoming = [(1, 10, 3, 9, 40, None, False), (2, 20, 3, 9, 60, None, False)]
    lake.write(CURRENT, spark.createDataFrame(incoming, schema))

    assert [(row[0], row[3]) for row in rows_of(lake, CURRENT.name)] == [(1, 2), (2, 9)]


def test_each_write_makes_a_snapshot_that_can_be_read_back(
    spark: SparkSession, lake: IcebergGold
) -> None:
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))
    first = lake.snapshot("dim_order_status")
    lake.write(SEEDS, seeds(spark, (1, "PENDING", False)))

    assert first is not None
    assert lake.snapshot("dim_order_status") != first
    at_first = lake.read_snapshot("dim_order_status", first)
    assert at_first is not None
    assert [tuple(row) for row in at_first.collect()] == [(1, "PENDING", True)]
    assert lake.read_snapshot("dim_order_status", 12345) is None


def test_a_snapshot_id_is_read_from_the_table_properties_without_a_spark_job(
    spark: SparkSession, lake: IcebergGold
) -> None:
    tracker = spark.sparkContext.statusTracker()
    lake.write(SEEDS, seeds(spark, (1, "PENDING", True)))
    before = set(tracker.getJobIdsForGroup(None))

    snapshot = lake.snapshot("dim_order_status")

    assert set(tracker.getJobIdsForGroup(None)) == before
    snapshots = spark.table(f"{lake.relation['dim_order_status']}.snapshots")
    assert snapshot == snapshots.orderBy("committed_at").collect()[-1]["snapshot_id"]


def test_drop_removes_every_table(spark: SparkSession, lake: IcebergGold) -> None:
    lake.drop()

    assert not any(spark.catalog.tableExists(relation) for relation in lake.relation.values())
