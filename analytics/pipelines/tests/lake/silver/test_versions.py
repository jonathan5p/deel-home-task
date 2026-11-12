import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.silver.versions_samples import (
    ENTITY_IDS,
    bronze_change,
    bronze_frame,
)
from pipelines.lake.bronze.entities import CUSTOMERS, ENTITIES, ORDER_ITEMS, PRODUCTS, Entity
from pipelines.lake.shared.columns import ChangeOp, MetadataColumn
from pipelines.lake.shared.tables import TABLE_TEMPLATES
from pipelines.lake.silver.versions import versions


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_a_version_has_the_columns_of_the_silver_template_in_order(
    spark: SparkSession, entity: Entity
) -> None:
    rows = versions(bronze_frame(spark, entity, []), entity)

    silver = TABLE_TEMPLATES["silver"].columns_for(entity.definition)
    assert rows.columns == [column.lake_name for column in silver]


def test_a_version_keeps_the_bronze_values_and_change_metadata(spark: SparkSession) -> None:
    change = bronze_change(ORDER_ITEMS, 5, ChangeOp.UPDATE)

    (row,) = versions(bronze_frame(spark, ORDER_ITEMS, [change]), ORDER_ITEMS).collect()

    copied = {
        name: value for name, value in row.asDict().items() if name != MetadataColumn.IS_DELETED
    }
    assert copied == {name: change[name] for name in copied}
    assert copied["quantity"] == 5


@pytest.mark.parametrize("op", ChangeOp, ids=str)
def test_only_a_delete_is_marked_deleted(spark: SparkSession, op: ChangeOp) -> None:
    change = bronze_change(PRODUCTS, 1, op)

    (row,) = versions(bronze_frame(spark, PRODUCTS, [change]), PRODUCTS).collect()

    assert row[MetadataColumn.IS_DELETED] is (op is ChangeOp.DELETE)


def test_each_bronze_change_becomes_one_version(spark: SparkSession) -> None:
    changes = [
        bronze_change(CUSTOMERS, 1, ChangeOp.SNAPSHOT),
        bronze_change(CUSTOMERS, 2, ChangeOp.UPDATE),
        bronze_change(CUSTOMERS, 3, ChangeOp.DELETE),
    ]

    rows = versions(bronze_frame(spark, CUSTOMERS, changes), CUSTOMERS)

    assert sorted(
        (row[MetadataColumn.COMMIT_SEQUENCE], row[MetadataColumn.IS_DELETED])
        for row in rows.collect()
    ) == [(1, False), (2, False), (3, True)]
