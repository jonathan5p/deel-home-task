import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.tables_setup import stored_description
from analytics.pipelines.tests.silver.job_setup import SilverSettingsFactory
from analytics.pipelines.tests.silver.views_setup import (
    CURRENT_COLUMNS,
    HISTORY_COLUMNS,
    Validity,
    append_versions,
    at,
    create_views,
    current,
    history,
    version,
)
from pipelines.bronze.entities import CUSTOMERS
from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.lake.tables import TABLE_TEMPLATES
from pipelines.silver.views import silver_views


def test_each_entity_has_a_history_view_then_a_current_view_next_to_its_table(
    silver_settings: SilverSettingsFactory,
) -> None:
    settings = silver_settings(CUSTOMERS, catalog="lake")

    views = silver_views(CUSTOMERS, settings)

    assert list(views) == ["lake.silver.customers_history", "lake.silver.customers_current"]
    assert "FROM lake.silver.customers\n" in views["lake.silver.customers_history"].query
    assert "FROM lake.silver.customers_history\n" in views["lake.silver.customers_current"].query


def test_the_views_store_their_descriptions_from_the_templates(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(spark, settings, [version(1, ChangeOp.CREATE)])

    create_views(spark, settings)

    for view, template in [("history", "silver_history"), ("current", "silver_current")]:
        stored = stored_description(spark, settings.table(f"customers_{view}"))
        columns = TABLE_TEMPLATES[template].columns_for(CUSTOMERS.definition)
        assert stored.comment == TABLE_TEMPLATES[template].describe(CUSTOMERS.definition)
        assert stored.columns == {column.lake_name: column.description for column in columns}


def test_the_views_have_the_columns_of_their_templates(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(spark, settings, [version(1, ChangeOp.CREATE)])

    create_views(spark, settings)

    assert spark.table(settings.table("customers_history")).columns == HISTORY_COLUMNS
    assert spark.table(settings.table("customers_current")).columns == CURRENT_COLUMNS


def test_the_first_version_of_each_key_is_never_folded(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(
        spark,
        settings,
        [version(1, ChangeOp.SNAPSHOT, key=1), version(2, ChangeOp.SNAPSHOT, key=2, content=1)],
    )

    create_views(spark, settings)

    assert history(spark, settings) == [
        Validity(1, 1, is_deleted=False, valid_from=1, valid_to=None, is_current=True),
        Validity(2, 2, is_deleted=False, valid_from=2, valid_to=None, is_current=True),
    ]


def test_a_re_snapshot_and_an_audit_only_update_are_folded(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(
        spark,
        settings,
        [
            version(1, ChangeOp.CREATE),
            version(2, ChangeOp.SNAPSHOT, content=1),
            version(3, ChangeOp.UPDATE, content=1) | {"updated_by": 42},
        ],
    )

    create_views(spark, settings)

    assert history(spark, settings) == [
        Validity(1, 1, is_deleted=False, valid_from=1, valid_to=None, is_current=True)
    ]


def test_a_delete_with_the_last_values_and_a_re_create_with_them_are_not_folded(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(
        spark,
        settings,
        [
            version(1, ChangeOp.CREATE),
            version(2, ChangeOp.DELETE, content=1),
            version(3, ChangeOp.CREATE, content=1),
        ],
    )

    create_views(spark, settings)

    assert history(spark, settings) == [
        Validity(1, 1, is_deleted=False, valid_from=1, valid_to=2, is_current=False),
        Validity(1, 2, is_deleted=True, valid_from=2, valid_to=3, is_current=False),
        Validity(1, 3, is_deleted=False, valid_from=3, valid_to=None, is_current=True),
    ]


def test_validity_runs_to_the_next_version_left_after_folding(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(
        spark,
        settings,
        [
            version(1, ChangeOp.CREATE),
            version(2, ChangeOp.SNAPSHOT, content=1),
            version(3, ChangeOp.UPDATE),
            version(4, ChangeOp.UPDATE, content=3),
            version(5, ChangeOp.UPDATE),
        ],
    )

    create_views(spark, settings)

    assert history(spark, settings) == [
        Validity(1, 1, is_deleted=False, valid_from=1, valid_to=3, is_current=False),
        Validity(1, 3, is_deleted=False, valid_from=3, valid_to=5, is_current=False),
        Validity(1, 5, is_deleted=False, valid_from=5, valid_to=None, is_current=True),
    ]


def test_a_late_version_lands_in_its_place_and_leaves_the_current_state(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(spark, settings, [version(1, ChangeOp.CREATE), version(3, ChangeOp.UPDATE)])
    create_views(spark, settings)
    before = current(spark, settings)

    append_versions(spark, settings, [version(2, ChangeOp.UPDATE)])

    assert history(spark, settings) == [
        Validity(1, 1, is_deleted=False, valid_from=1, valid_to=2, is_current=False),
        Validity(1, 2, is_deleted=False, valid_from=2, valid_to=3, is_current=False),
        Validity(1, 3, is_deleted=False, valid_from=3, valid_to=None, is_current=True),
    ]
    assert current(spark, settings) == before
    assert [row[MetadataColumn.COMMIT_SEQUENCE] for row in before] == [3]


def test_the_current_view_has_the_current_version_of_each_live_key(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_view_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_view_catalog)
    append_versions(
        spark,
        settings,
        [
            version(1, ChangeOp.CREATE, key=1),
            version(2, ChangeOp.UPDATE, key=1),
            version(3, ChangeOp.SNAPSHOT, key=1, content=2),
            version(4, ChangeOp.CREATE, key=2),
            version(5, ChangeOp.DELETE, key=2, content=4),
            version(6, ChangeOp.CREATE, key=3),
            version(7, ChangeOp.DELETE, key=3, content=6),
            version(8, ChangeOp.CREATE, key=3, content=6),
        ],
    )

    create_views(spark, settings)

    rows = current(spark, settings)
    assert [(row[CUSTOMERS.lake_key], row[MetadataColumn.COMMIT_SEQUENCE]) for row in rows] == [
        (1, 2),
        (3, 8),
    ]
    assert rows[0][MetadataColumn.VALID_FROM] == at(2)
