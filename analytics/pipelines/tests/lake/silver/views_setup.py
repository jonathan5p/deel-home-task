from datetime import datetime, timedelta
from typing import NamedTuple

from pyspark.sql import Row, SparkSession

from analytics.pipelines.tests.lake.silver.deduplication_setup import ENTITY, KEY, change
from analytics.pipelines.tests.lake.silver.versions_samples import (
    CHANGED_AT,
    BronzeChange,
    bronze_frame,
)
from pipelines.lake.shared.columns import ChangeOp, MetadataColumn
from pipelines.lake.shared.tables import (
    TABLE_TEMPLATES,
    described_schema,
    ensure_table,
    table_definition,
)
from pipelines.lake.silver.job import SILVER
from pipelines.lake.silver.versions import versions
from pipelines.settings import SilverSettings

HISTORY_COLUMNS = [
    column.lake_name for column in TABLE_TEMPLATES["silver_history"].columns_for(ENTITY.definition)
]
CURRENT_COLUMNS = [
    column.lake_name for column in TABLE_TEMPLATES["silver_current"].columns_for(ENTITY.definition)
]
CUSTOMER_COLUMNS = """customer_id,
    customer_name,
    is_active,
    customer_address,
    updated_at,
    updated_by,
    created_at,
    created_by,
    _commit_sequence,
    _change_op,
    _changed_at,
    _content_hash"""
BY_KEY = "PARTITION BY customer_id ORDER BY _commit_sequence"
CUSTOMERS_HISTORY_SQL = f"""WITH versions AS (
    SELECT
        *,
        coalesce(
            _content_hash = lag(_content_hash) OVER ({BY_KEY})
            AND _is_deleted = lag(_is_deleted) OVER ({BY_KEY}),
            false
        ) AS _folded
    FROM lake.silver.customers
)
SELECT
    {CUSTOMER_COLUMNS},
    _is_deleted,
    _changed_at AS _valid_from,
    lead(_changed_at) OVER ({BY_KEY}) AS _valid_to,
    lead(_commit_sequence) OVER ({BY_KEY}) IS NULL AS _is_current
FROM versions
WHERE NOT _folded"""
CUSTOMERS_CURRENT_SQL = f"""SELECT
    {CUSTOMER_COLUMNS},
    _valid_from
FROM lake.silver.customers_history
WHERE _is_current AND NOT _is_deleted"""


def at(sequence: int) -> datetime:
    """The commit time of the change at `sequence`: one second per position."""
    return CHANGED_AT + timedelta(seconds=sequence)


def version(
    sequence: int, op: ChangeOp, *, key: int = 1, content: int | None = None
) -> BronzeChange:
    """A customers change of `key` committed at `sequence`, whose content (and so its hash) is
    the one `content` gives, by default its own."""
    return change(sequence, op, key=key, values=content) | {MetadataColumn.CHANGED_AT: at(sequence)}


def append_versions(
    spark: SparkSession, settings: SilverSettings, changes: list[BronzeChange]
) -> None:
    """Append the version rows of the changes to the silver table, created as the job creates it."""
    table = settings.table(ENTITY.name)
    definition = table_definition(SILVER, ENTITY.definition)
    rows = versions(bronze_frame(spark, ENTITY, changes), ENTITY)
    ensure_table(spark, table, described_schema(table, rows.schema, definition), definition)
    rows.writeTo(table).append()


class Validity(NamedTuple):
    key: int
    commit_sequence: int
    is_deleted: bool
    valid_from: int
    valid_to: int | None
    is_current: bool


def position(time: datetime) -> int:
    """The commit sequence whose commit time `at` gives."""
    return int((time - CHANGED_AT).total_seconds())


def history(spark: SparkSession, settings: SilverSettings) -> list[Validity]:
    """The history view's versions by key and commit sequence, with their validity times as
    commit sequences."""
    rows = spark.table(settings.table(f"{ENTITY.name}_history")).collect()
    validities = [
        Validity(
            key=row[KEY],
            commit_sequence=row[MetadataColumn.COMMIT_SEQUENCE],
            is_deleted=row[MetadataColumn.IS_DELETED],
            valid_from=position(row[MetadataColumn.VALID_FROM]),
            valid_to=None
            if row[MetadataColumn.VALID_TO] is None
            else position(row[MetadataColumn.VALID_TO]),
            is_current=row[MetadataColumn.IS_CURRENT],
        )
        for row in rows
    ]
    return sorted(validities)


def current(spark: SparkSession, settings: SilverSettings) -> list[Row]:
    view = settings.table(f"{ENTITY.name}_current")
    return spark.table(view).orderBy(KEY).collect()
