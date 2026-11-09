from collections.abc import Iterable, Mapping
from typing import NamedTuple

from pipelines.bronze.columns import (
    CAPTURED_AT,
    CHANGE_OP,
    CHANGED_AT,
    COMMIT_SEQUENCE,
    CONTENT_HASH,
    FAILURE_REASON,
    LANDING_FILE,
    PUBLISHED_AT,
    QUARANTINED_AT,
    RAW_KEY,
    RAW_VALUE,
    RECORD_SOURCE,
    SOURCE_OFFSET,
    SOURCE_PARTITION,
)
from pipelines.bronze.entities import Entity

METADATA_COLUMN_DESCRIPTIONS: Mapping[str, str] = {
    RECORD_SOURCE: "Kafka topic the change event was read from",
    SOURCE_PARTITION: "Kafka partition of the record",
    SOURCE_OFFSET: "Offset of the record in its Kafka partition",
    PUBLISHED_AT: "Kafka record timestamp (UTC); the table is partitioned by its day",
    CHANGE_OP: "Kind of change: create, update, delete or snapshot (Debezium op c, u, d or r)",
    COMMIT_SEQUENCE: (
        "Position of the change in the source's commit log (Debezium source.lsn);"
        " increases in commit order"
    ),
    CHANGED_AT: "Time the change was committed in the source database (Debezium source.ts_ms)",
    CAPTURED_AT: "Time Debezium processed the change (envelope ts_ms)",
    CONTENT_HASH: (
        "Hex SHA-256 of the content columns (key and audit columns excluded);"
        " unchanged content keeps its hash"
    ),
    LANDING_FILE: "Path of the landing Avro file the record was read from",
    RAW_KEY: "Raw Kafka record key, as text",
    RAW_VALUE: "Raw Kafka record value, as text",
    FAILURE_REASON: "First validation check the record failed",
    QUARANTINED_AT: "Processing time of the micro-batch that quarantined the record",
}


class TableDescription(NamedTuple):
    comment: str
    columns: Mapping[str, str]

    def undescribed(self, names: Iterable[str]) -> list[str]:
        return [name for name in names if not self.columns.get(name, "").strip()]


class UndescribedColumnError(ValueError):
    def __init__(self, table: str, columns: Iterable[str]) -> None:
        super().__init__(f"Columns of {table} have no description: {', '.join(columns)}")


def bronze_table_description(entity: Entity) -> TableDescription:
    business = {column.bronze_name: column.description for column in entity.columns}
    return TableDescription(
        comment=(
            f"Change log of source table {entity.name}: one typed row per Debezium change event"
            " (create, update, delete or snapshot), append-only and not deduplicated"
        ),
        columns={**METADATA_COLUMN_DESCRIPTIONS, **business},
    )


def quarantine_table_description(entity: Entity) -> TableDescription:
    return TableDescription(
        comment=(
            f"Change events of source table {entity.name} that failed bronze validation,"
            " kept raw with the reason"
        ),
        columns=METADATA_COLUMN_DESCRIPTIONS,
    )
