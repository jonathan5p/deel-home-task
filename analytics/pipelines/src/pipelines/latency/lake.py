from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from pipelines.lake.shared.columns import MetadataColumn
from pipelines.latency.layers import Layer
from pipelines.latency.probe import Change, ProbeTarget
from pipelines.latency.sql import Row, identifier, relation, single_integer
from pipelines.schemas.definitions import TableTemplate, load_table_templates

PARTITION_VALUES: Mapping[str, Callable[[Change], int]] = {
    MetadataColumn.PUBLISHED_AT: lambda change: change.published_at,
    MetadataColumn.CHANGED_AT: lambda change: change.changed_at,
    MetadataColumn.CAPTURED_AT: lambda change: change.captured_at,
}


def timestamp(epoch_millis: int) -> str:
    """An exact TIMESTAMPTZ literal, whatever the session time zone."""
    seconds, millis = divmod(epoch_millis, 1000)
    return f"(to_timestamp({seconds}) + to_milliseconds({millis}))"


def lake_lookup_query(
    table: str, template: TableTemplate, target: ProbeTarget, change: Change
) -> str:
    """Count the rows of `table` that hold the change; the template's partition column, when
    the change knows its value, lets the reader skip every other day."""
    conditions = [
        f"{identifier(target.entity.lake_key)} = {target.key}",
        f"{identifier(MetadataColumn.COMMIT_SEQUENCE)} = {change.commit_sequence}",
    ]
    partition = template.partitioned_by
    if partition is not None and partition in PARTITION_VALUES:
        value = PARTITION_VALUES[partition](change)
        conditions.append(f"{identifier(partition)} = {timestamp(value)}")
    return f"SELECT count(*) FROM {table} WHERE {' AND '.join(conditions)}"


@dataclass(frozen=True, slots=True)
class LakeLookup:
    """Whether a lake layer's Iceberg table of an entity holds a change, read through a catalog
    attached under the lake's catalog name."""

    catalog: str
    template: TableTemplate
    query: Callable[[str], Sequence[Row]]

    def table(self, target: ProbeTarget) -> str:
        name = self.template.table_name(target.entity.definition)
        return relation(self.catalog, self.template.layer, name)

    def visible(self, target: ProbeTarget, change: Change) -> bool:
        sql = lake_lookup_query(self.table(target), self.template, target, change)
        return single_integer(self.query(sql)) > 0


def lake_lookups(catalog: str, query: Callable[[str], Sequence[Row]]) -> dict[Layer, LakeLookup]:
    """The bronze and silver lookups, each on the table its layer keeps per entity."""
    templates = load_table_templates()
    return {
        layer: LakeLookup(catalog, templates[layer], query)
        for layer in (Layer.BRONZE, Layer.SILVER)
    }
