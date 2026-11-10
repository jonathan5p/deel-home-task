import pytest

pytest.importorskip("pyspark")

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from analytics.pipelines.tests.lake.tables_setup import stored_description, table_ddl
from analytics.pipelines.tests.silver.job_setup import (
    SilverSettingsFactory,
    commit_sequences,
    local,
    no_bronze_rows,
    run_until_caught_up,
    silver_rows,
    topic,
    write_bronze,
)
from analytics.pipelines.tests.silver.versions_samples import ENTITY_IDS, bronze_change
from pipelines.bronze.entities import CUSTOMERS, ENTITIES, PRODUCTS, Entity
from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.lake.tables import TABLE_TEMPLATES, table_definition
from pipelines.lake.types import SPARK_TYPES
from pipelines.schemas.render import avro_field, render_table_schema
from pipelines.silver.job import SILVER, entity_stream, run


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_every_column_of_every_silver_table_is_described(
    spark: SparkSession, silver_settings: SilverSettingsFactory, entity: Entity
) -> None:
    settings = silver_settings(entity)

    stream = entity_stream(spark, settings, topic(entity), no_bronze_rows(entity))

    schema = stream.table_schema()
    assert all(field.metadata["comment"] for field in schema)
    assert schema.fieldNames() == stream.rows.schema.fieldNames()
    assert stream.definition == table_definition(SILVER, entity.definition)


@pytest.mark.parametrize("entity", ENTITIES, ids=ENTITY_IDS)
def test_the_published_table_schemas_have_the_columns_and_types_the_job_writes(
    spark: SparkSession, silver_settings: SilverSettingsFactory, entity: Entity
) -> None:
    settings = silver_settings(entity)

    stream = entity_stream(spark, settings, topic(entity), no_bronze_rows(entity))

    template = TABLE_TEMPLATES["silver"]
    columns = template.columns_for(entity.definition)
    published = render_table_schema(template, entity.definition)
    assert [(c.lake_name, SPARK_TYPES[c.type]) for c in columns] == [
        (field.name, field.dataType) for field in stream.rows.schema
    ]
    assert published["fields"] == [avro_field(column) for column in columns]
    assert stream.table.endswith(f".silver.{published['name']}")


def test_each_bronze_change_becomes_one_version_row(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    changes = [
        bronze_change(CUSTOMERS, 1, ChangeOp.SNAPSHOT),
        bronze_change(CUSTOMERS, 2, ChangeOp.UPDATE),
        bronze_change(CUSTOMERS, 3, ChangeOp.DELETE),
    ]
    write_bronze(spark, settings, CUSTOMERS, changes)

    run_until_caught_up(spark, settings)

    rows = silver_rows(spark, settings, CUSTOMERS)
    assert [row[MetadataColumn.IS_DELETED] for row in rows] == [False, False, True]
    for row, change in zip(rows, changes, strict=True):
        copied = {name: change[name] for name in row.asDict() if name != MetadataColumn.IS_DELETED}
        assert {name: row[name] for name in copied} == copied


def test_each_entity_gets_its_own_silver_table_and_checkpoint(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, PRODUCTS, catalog=iceberg_catalog)
    for entity in (CUSTOMERS, PRODUCTS):
        write_bronze(spark, settings, entity, [bronze_change(entity, 1, ChangeOp.CREATE)])

    run_until_caught_up(spark, settings)

    for entity in (CUSTOMERS, PRODUCTS):
        assert commit_sequences(silver_rows(spark, settings, entity)) == [1]
    checkpoints = local(settings.lake.checkpoint_root) / "silver"
    assert {path.name for path in checkpoints.iterdir()} == {"customers", "products"}
    assert all((path / "commits").is_dir() for path in checkpoints.iterdir())


def test_a_restart_appends_only_the_changes_since(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)])
    run_until_caught_up(spark, settings)

    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 2, ChangeOp.UPDATE)])
    run_until_caught_up(spark, settings)
    run_until_caught_up(spark, settings)

    assert commit_sequences(silver_rows(spark, settings, CUSTOMERS)) == [1, 2]


def test_silver_tables_are_partitioned_by_change_day_and_store_every_description(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(PRODUCTS, catalog=iceberg_catalog)
    write_bronze(spark, settings, PRODUCTS, [bronze_change(PRODUCTS, 1, ChangeOp.CREATE)])

    run_until_caught_up(spark, settings)

    table = settings.table(PRODUCTS.name)
    ddl = table_ddl(spark, table)
    assert f"PARTITIONED BY (days({MetadataColumn.CHANGED_AT}))" in ddl
    assert "'write.distribution-mode' = 'none'" in ddl
    definition = table_definition(SILVER, PRODUCTS.definition)
    stored = stored_description(spark, table)
    assert stored.comment == definition.comment
    assert stored.columns == {
        name: column.description for name, column in definition.columns.items()
    }


def test_the_job_fails_before_creating_any_table_while_a_bronze_table_is_missing(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, PRODUCTS, catalog=iceberg_catalog)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)])

    with pytest.raises(AnalysisException, match="products"):
        run(spark, settings, available_now=True)

    assert not spark.catalog.tableExists(settings.table(CUSTOMERS.name))


def test_the_job_keeps_running_on_its_processing_time_trigger(
    spark: SparkSession, silver_settings: SilverSettingsFactory, iceberg_catalog: str
) -> None:
    settings = silver_settings(CUSTOMERS, catalog=iceberg_catalog)
    write_bronze(spark, settings, CUSTOMERS, [bronze_change(CUSTOMERS, 1, ChangeOp.CREATE)])

    queries = run(spark, settings)
    try:
        assert [query.name for query in queries] == ["silver customers"]
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()
