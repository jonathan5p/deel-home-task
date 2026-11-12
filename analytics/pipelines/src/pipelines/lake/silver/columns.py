from pipelines.lake.shared.columns import MetadataColumn

COPIED_METADATA_COLUMNS = (
    MetadataColumn.COMMIT_SEQUENCE,
    MetadataColumn.CHANGE_OP,
    MetadataColumn.CHANGED_AT,
    MetadataColumn.CONTENT_HASH,
)
SILVER_METADATA_COLUMNS = (*COPIED_METADATA_COLUMNS, MetadataColumn.IS_DELETED)
