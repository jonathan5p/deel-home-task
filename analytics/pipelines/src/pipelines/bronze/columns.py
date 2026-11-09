from enum import StrEnum


class MetadataColumn(StrEnum):
    RECORD_SOURCE = "_record_source"
    SOURCE_PARTITION = "_source_partition"
    SOURCE_OFFSET = "_source_offset"
    PUBLISHED_AT = "_published_at"
    CHANGE_OP = "_change_op"
    COMMIT_SEQUENCE = "_commit_sequence"
    CHANGED_AT = "_changed_at"
    CAPTURED_AT = "_captured_at"
    CONTENT_HASH = "_content_hash"
    LANDING_FILE = "_landing_file"
    FAILURE_REASON = "_failure_reason"
    QUARANTINED_AT = "_quarantined_at"


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
