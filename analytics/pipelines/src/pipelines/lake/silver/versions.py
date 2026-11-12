from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.lake.bronze.entities import Entity
from pipelines.lake.shared.columns import ChangeOp, MetadataColumn
from pipelines.lake.silver.columns import COPIED_METADATA_COLUMNS


def versions(bronze: DataFrame, entity: Entity) -> DataFrame:
    is_deleted = F.col(MetadataColumn.CHANGE_OP) == F.lit(str(ChangeOp.DELETE))
    return bronze.select(
        *(column.lake_name for column in entity.columns),
        *COPIED_METADATA_COLUMNS,
        is_deleted.alias(MetadataColumn.IS_DELETED),
    )
