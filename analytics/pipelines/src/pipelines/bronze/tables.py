from pipelines.bronze.entities import Entity
from pipelines.lake.columns import MetadataColumn
from pipelines.lake.tables import TableDefinition, table_definition


def bronze_table(entity: Entity) -> TableDefinition:
    return table_definition("bronze", entity.definition, partitioned_by=MetadataColumn.PUBLISHED_AT)


def quarantine_table(entity: Entity) -> TableDefinition:
    return table_definition(
        "bronze_quarantine", entity.definition, partitioned_by=MetadataColumn.PUBLISHED_AT
    )
