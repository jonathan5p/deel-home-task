from pipelines.bronze.entities import Entity
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.tables import TableDefinition, table_definition


def silver_table(entity: Entity) -> TableDefinition:
    return table_definition("silver", entity.definition, partitioned_by=MetadataColumn.CHANGED_AT)
