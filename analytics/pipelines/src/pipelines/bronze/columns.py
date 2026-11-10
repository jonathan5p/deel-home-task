from enum import StrEnum

from pipelines.lake.columns import MetadataColumn


class RawColumn(StrEnum):
    """The Kafka record's own columns, which quarantine tables keep as text under their names."""

    KEY = "key"
    VALUE = "value"


SOURCE_COORDINATES = (
    MetadataColumn.RECORD_SOURCE,
    MetadataColumn.SOURCE_PARTITION,
    MetadataColumn.SOURCE_OFFSET,
    MetadataColumn.PUBLISHED_AT,
)
BRONZE_METADATA_COLUMNS = (
    *SOURCE_COORDINATES,
    MetadataColumn.CHANGE_OP,
    MetadataColumn.COMMIT_SEQUENCE,
    MetadataColumn.CHANGED_AT,
    MetadataColumn.CAPTURED_AT,
    MetadataColumn.CONTENT_HASH,
)
QUARANTINE_COLUMNS = (
    *SOURCE_COORDINATES,
    MetadataColumn.LANDING_FILE,
    RawColumn.KEY,
    RawColumn.VALUE,
    MetadataColumn.FAILURE_REASON,
)
