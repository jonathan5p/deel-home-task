import io
from collections.abc import Iterable, Mapping

from fastavro import parse_schema, writer

LANDING_AVRO_SCHEMA = parse_schema(
    {
        "type": "record",
        "name": "topLevelRecord",
        "fields": [
            {"name": "key", "type": ["bytes", "null"]},
            {"name": "value", "type": ["bytes", "null"]},
            {"name": "topic", "type": ["string", "null"]},
            {"name": "partition", "type": ["int", "null"]},
            {"name": "offset", "type": ["long", "null"]},
            {
                "name": "timestamp",
                "type": [{"type": "long", "logicalType": "timestamp-micros"}, "null"],
            },
            {"name": "timestampType", "type": ["int", "null"]},
        ],
    }
)


def landing_avro(records: Iterable[Mapping[str, object]]) -> bytes:
    """Encode records as one Avro file with the schema Spark writes for landing records."""
    buffer = io.BytesIO()
    writer(buffer, LANDING_AVRO_SCHEMA, records)
    return buffer.getvalue()
