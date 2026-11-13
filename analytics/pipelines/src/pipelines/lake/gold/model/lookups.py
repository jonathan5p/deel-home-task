from collections.abc import Iterable, Mapping, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.lake.gold.facts import KeyLookup

CODE = "__lookup_code"


def _value(key: str) -> str:
    return f"__lookup_{key}"


def _fallback(key: str) -> str:
    return f"__fallback_{key}"


def with_shared_lookups(
    rows: DataFrame, lookups: Sequence[KeyLookup], seeded: DataFrame
) -> DataFrame:
    """`rows` with the columns of `lookups` that read the same code from the same staged column
    of `seeded`: each the `key` of the row whose `code` equals the staged column, or of the
    `fallback` code's row when none does.

    One join fills them all. The seeded dimensions are small, so the joins are broadcast, and
    the broadcast rows are named by the dimension's columns only, so lookups of the same
    dimension from different staged columns broadcast the same rows once.
    """
    first = lookups[0]
    keys = list(dict.fromkeys(lookup.key for lookup in lookups))
    matches = seeded.select(
        F.col(first.code).alias(CODE), *(F.col(k).alias(_value(k)) for k in keys)
    )
    looked_up = rows.drop(*(lookup.column for lookup in lookups)).join(
        F.broadcast(matches), F.col(first.source) == F.col(CODE), "left"
    )
    temporary = [CODE, *(_value(key) for key in keys)]
    values = {key: F.col(_value(key)) for key in keys}
    if first.fallback is not None:
        default = seeded.where(F.col(first.code) == first.fallback).select(
            *(F.col(key).alias(_fallback(key)) for key in keys)
        )
        looked_up = looked_up.join(F.broadcast(default), F.lit(True), "left")
        temporary += [_fallback(key) for key in keys]
        values = {key: F.coalesce(F.col(_value(key)), F.col(_fallback(key))) for key in keys}
    for lookup in lookups:
        looked_up = looked_up.withColumn(lookup.column, values[lookup.key])
    return looked_up.drop(*temporary)


def with_key_lookups(
    rows: DataFrame, lookups: Iterable[KeyLookup], seeded: Mapping[str, DataFrame]
) -> DataFrame:
    """`rows` with every lookup's column; `seeded` holds each seeded dimension by table name."""
    shared: dict[tuple[str, str, str, str | None], list[KeyLookup]] = {}
    for lookup in lookups:
        group = (lookup.dimension.name, lookup.source, lookup.code, lookup.fallback)
        shared.setdefault(group, []).append(lookup)
    for (dimension, *_), group in shared.items():
        rows = with_shared_lookups(rows, group, seeded[dimension])
    return rows
