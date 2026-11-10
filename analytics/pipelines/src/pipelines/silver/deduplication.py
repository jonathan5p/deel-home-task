from pyspark.sql import DataFrame

from pipelines.bronze.entities import Entity
from pipelines.lake.columns import MetadataColumn


def drop_duplicate_changes(rows: DataFrame, entity: Entity) -> DataFrame:
    # No watermark: late changes must never be dropped, so the state keeps every change it saw.
    return rows.dropDuplicates(
        [entity.key_column, MetadataColumn.COMMIT_SEQUENCE, MetadataColumn.CHANGE_OP]
    )
