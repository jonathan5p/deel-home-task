from pyspark.sql import DataFrame

from pipelines.bronze.entities import Entity
from pipelines.lake.columns import MetadataColumn


def drop_duplicate_changes(rows: DataFrame, entity: Entity) -> DataFrame:
    return rows.dropDuplicates(
        [entity.lake_key, MetadataColumn.COMMIT_SEQUENCE, MetadataColumn.CHANGE_OP]
    )
