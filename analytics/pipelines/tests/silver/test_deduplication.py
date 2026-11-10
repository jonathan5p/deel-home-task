from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession

from analytics.pipelines.tests.silver.deduplication_setup import (
    ENTITY,
    change,
    identities,
    redelivered,
    run_per_batch,
)
from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.silver.deduplication import drop_duplicate_changes


def deduplicated(rows: DataFrame) -> DataFrame:
    return drop_duplicate_changes(rows, ENTITY)


def test_a_change_delivered_twice_in_one_micro_batch_is_kept_once(
    spark: SparkSession, tmp_path: Path
) -> None:
    created = change(1, ChangeOp.CREATE)

    (appended,) = run_per_batch(
        spark, deduplicated, [[created, redelivered(created, 50)]], tmp_path
    )

    assert identities(appended) == [(1, 1, ChangeOp.CREATE)]


def test_a_change_delivered_again_in_a_later_micro_batch_is_dropped(
    spark: SparkSession, tmp_path: Path
) -> None:
    created = change(1, ChangeOp.CREATE)

    appended = run_per_batch(spark, deduplicated, [[created], [redelivered(created, 50)]], tmp_path)

    assert [identities(rows) for rows in appended] == [[(1, 1, ChangeOp.CREATE)], []]


def test_a_replay_appends_only_the_changes_not_seen_before(
    spark: SparkSession, tmp_path: Path
) -> None:
    applied = [change(1, ChangeOp.SNAPSHOT), change(2, ChangeOp.UPDATE), change(3, ChangeOp.UPDATE)]
    replay = [*(redelivered(original, 50) for original in applied), change(4, ChangeOp.UPDATE)]

    _, appended = run_per_batch(spark, deduplicated, [applied, replay], tmp_path)

    assert identities(appended) == [(1, 4, ChangeOp.UPDATE)]


def test_changes_out_of_order_in_one_micro_batch_are_all_kept(
    spark: SparkSession, tmp_path: Path
) -> None:
    batch = [change(3, ChangeOp.UPDATE), change(1, ChangeOp.CREATE), change(2, ChangeOp.UPDATE)]

    (appended,) = run_per_batch(spark, deduplicated, [batch], tmp_path)

    assert identities(appended) == [
        (1, 1, ChangeOp.CREATE),
        (1, 2, ChangeOp.UPDATE),
        (1, 3, ChangeOp.UPDATE),
    ]


def test_a_late_change_older_than_the_stored_versions_is_appended(
    spark: SparkSession, tmp_path: Path
) -> None:
    batches = [
        [change(1, ChangeOp.CREATE), change(3, ChangeOp.UPDATE)],
        [change(2, ChangeOp.UPDATE)],
    ]

    _, appended = run_per_batch(spark, deduplicated, batches, tmp_path)

    assert identities(appended) == [(1, 2, ChangeOp.UPDATE)]


def test_a_re_snapshot_and_an_audit_only_update_are_kept_as_versions_of_their_own(
    spark: SparkSession, tmp_path: Path
) -> None:
    created = change(1, ChangeOp.CREATE)
    re_snapshot = change(5, ChangeOp.SNAPSHOT, values=1)
    audit_only = change(6, ChangeOp.UPDATE, values=1) | {"updated_by": 42}

    _, appended = run_per_batch(
        spark, deduplicated, [[created], [re_snapshot, audit_only]], tmp_path
    )

    assert identities(appended) == [(1, 5, ChangeOp.SNAPSHOT), (1, 6, ChangeOp.UPDATE)]
    hashes = {row[MetadataColumn.CONTENT_HASH] for row in appended}
    assert hashes == {created[MetadataColumn.CONTENT_HASH]}


def test_a_delete_with_the_last_values_and_a_re_create_are_kept(
    spark: SparkSession, tmp_path: Path
) -> None:
    batches = [
        [change(1, ChangeOp.CREATE)],
        [change(2, ChangeOp.DELETE, values=1)],
        [change(3, ChangeOp.CREATE, values=1)],
    ]

    appended = run_per_batch(spark, deduplicated, batches, tmp_path)

    assert [identities(rows) for rows in appended] == [
        [(1, 1, ChangeOp.CREATE)],
        [(1, 2, ChangeOp.DELETE)],
        [(1, 3, ChangeOp.CREATE)],
    ]


def test_different_keys_sharing_a_commit_sequence_are_all_kept(
    spark: SparkSession, tmp_path: Path
) -> None:
    snapshot = [change(10, ChangeOp.SNAPSHOT, key=key) for key in (1, 2, 3)]

    (appended,) = run_per_batch(spark, deduplicated, [snapshot], tmp_path)

    assert identities(appended) == [
        (1, 10, ChangeOp.SNAPSHOT),
        (2, 10, ChangeOp.SNAPSHOT),
        (3, 10, ChangeOp.SNAPSHOT),
    ]


def test_another_operation_at_the_same_commit_sequence_is_another_change(
    spark: SparkSession, tmp_path: Path
) -> None:
    batch = [change(1, ChangeOp.CREATE), change(1, ChangeOp.DELETE)]

    (appended,) = run_per_batch(spark, deduplicated, [batch], tmp_path)

    assert identities(appended) == [(1, 1, ChangeOp.CREATE), (1, 1, ChangeOp.DELETE)]
