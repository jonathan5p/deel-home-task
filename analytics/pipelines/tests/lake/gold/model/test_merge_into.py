import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_merge_into_updates_only_newer_rows_and_inserts_new_keys_in_an_iceberg_table(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.gold.current_things"
    spark.createDataFrame(
        [(1, 10, "stored"), (2, 20, "stored")], "thing_id long, _commit_sequence long, name string"
    ).writeTo(table).create()
    batch = spark.createDataFrame(
        [(1, 5, "older"), (2, 25, "newer"), (3, 30, "new")],
        "thing_id long, _commit_sequence long, name string",
    ).alias("source")

    (
        batch.mergeInto(table, F.col("source.thing_id") == F.col(f"{table}.thing_id"))
        .whenMatched(F.col("source._commit_sequence") > F.col(f"{table}._commit_sequence"))
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )

    assert sorted(tuple(row) for row in spark.table(table).collect()) == [
        (1, 10, "stored"),
        (2, 25, "newer"),
        (3, 30, "new"),
    ]


def test_merge_into_deletes_removed_report_rows_matching_null_keys(
    spark: SparkSession, iceberg_catalog: str
) -> None:
    table = f"{iceberg_catalog}.gold.report_things"
    spark.createDataFrame(
        [(None, 2), (1, 3), (2, 4)], "product_id long, pending_items long"
    ).writeTo(table).create()
    report = spark.createDataFrame(
        [(None, 1, False), (1, None, True), (3, 5, False), (4, None, True)],
        "product_id long, pending_items long, _is_removed boolean",
    ).alias("source")
    removed = F.col("source._is_removed")

    (
        report.mergeInto(table, F.col("source.product_id").eqNullSafe(F.col(f"{table}.product_id")))
        .whenMatched(removed)
        .delete()
        .whenMatched()
        .update({"pending_items": F.col("source.pending_items")})
        .whenNotMatched(~removed)
        .insert(
            {
                "product_id": F.col("source.product_id"),
                "pending_items": F.col("source.pending_items"),
            }
        )
        .merge()
    )

    rows = [tuple(row) for row in spark.table(table).collect()]
    assert sorted(rows, key=lambda row: (row[0] is not None, row[0] or 0)) == [
        (None, 1),
        (2, 4),
        (3, 5),
    ]
