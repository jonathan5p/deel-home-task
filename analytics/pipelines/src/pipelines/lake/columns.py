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
    IS_DELETED = "_is_deleted"


class ChangeOp(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT = "snapshot"
