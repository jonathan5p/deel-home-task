from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.schemas.landing import KAFKA_RECORD_SCHEMA

__all__ = ["KAFKA_RECORD_SCHEMA"]


def __getattr__(name: str) -> object:
    # The landing schema needs PySpark; resolving it on first use keeps the Avro definitions in
    # this package loadable where PySpark is not installed, such as the infra tests on the host.
    if name == "KAFKA_RECORD_SCHEMA":
        from pipelines.schemas.landing import KAFKA_RECORD_SCHEMA

        return KAFKA_RECORD_SCHEMA
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
