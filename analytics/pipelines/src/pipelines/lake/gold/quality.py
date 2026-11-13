from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from typing import NamedTuple, Protocol

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common.settings import AnalyticsDbSettings
from pipelines.lake.gold.dimensions import DIMENSIONS, FACTS, STORED
from pipelines.lake.gold.facts import ChangeFact, FactColumn
from pipelines.lake.gold.job import iceberg_gold
from pipelines.lake.gold.model.columns import IS_REMOVED, VersionColumn
from pipelines.lake.gold.model.lookups import with_as_of_lookup
from pipelines.lake.gold.model.reports import (
    open_orders_by_delivery_date_and_status,
    pending_items_by_product,
    pending_orders_by_customer,
    products_of_orders,
    top_customers_by_pending_orders,
    top_delivery_dates_by_open_orders,
)
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VALID_FROM, any_of
from pipelines.lake.gold.retention import retained_from
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.serving import SYNC_STATE
from pipelines.lake.gold.sink import jdbc_options
from pipelines.lake.gold.stored import (
    OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS,
    PENDING_ITEMS_BY_PRODUCT,
    PENDING_ORDERS_BY_CUSTOMER,
    SEED_KEYS,
    TOP_CUSTOMERS,
    TOP_DELIVERY_DATES,
    StoredTable,
)
from pipelines.quality.checks import Check, CheckResult
from pipelines.settings import GoldSettings

SAMPLE_SIZE = 10
MAX_READS = 3
PARITY_METRICS = ("lake_rows", "postgres_rows", "lake_only", "postgres_only")
INVARIANT_METRICS = ("rows", "violations")
CHANGED_AT = str(FactColumn.CHANGED_AT)
REPORTS = (
    OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS,
    PENDING_ORDERS_BY_CUSTOMER,
    PENDING_ITEMS_BY_PRODUCT,
    TOP_DELIVERY_DATES,
    TOP_CUSTOMERS,
)
RECOUNT_SOURCES = ("current_orders", "current_order_items", "dim_order_status", "dim_customer")

COPY_MATCHES_ICEBERG = Check(
    "gold_copy_matches_iceberg",
    "The PostgreSQL gold_internal table holds exactly the rows of its Iceberg table at the"
    " snapshot gold_internal.sync_state records for it; history is compared from the first day"
    " of the retention window, which PostgreSQL keeps.",
)
ONE_CURRENT_VERSION = Check(
    "gold_one_current_version",
    "Each business key has at most one current row: one version with _is_current in a"
    " dimension or history fact, one row in a current-state table.",
)
NO_OVERLAPPING_RANGES = Check(
    "gold_no_overlapping_validity",
    "The validity ranges of a key's versions that are not folded do not overlap: each version"
    " ends at or before the start of the next one, and only the last one is open.",
)
RESOLVED_LOOKUPS = Check(
    "gold_lookups_resolve",
    "Every status and date key names a row of its seeded dimension, or is null where the model"
    " allows it (an unknown status takes the UNKNOWN row); every history fact's as-of columns"
    " are those of the dimension version valid when the change was committed, as the gold model"
    " resolves them, so they are null only when the key had no version yet or a deleted one.",
)
REPORTS_MATCH_RECOUNT = Check(
    "gold_reports_match_recount",
    "The report table equals a recount of the current-state tables.",
)

type Sample = tuple[Mapping[str, object], ...]
type PostgresReader = Callable[[SparkSession, str], DataFrame]
type Today = Callable[[], date]
type Frames = Mapping[str, DataFrame | None]


class IcebergTables(Protocol):
    """The gold tables in Iceberg, as the check reads them."""

    def read(self, name: str) -> DataFrame: ...

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None: ...


@dataclass(frozen=True, slots=True)
class GoldReaders:
    read_postgres: PostgresReader
    lake: IcebergTables


class Differences(NamedTuple):
    expected_only: DataFrame
    actual_only: DataFrame


class Matching(NamedTuple):
    lake_rows: int
    postgres_rows: int
    lake_only: int
    postgres_only: int
    samples: Mapping[str, Sample]

    @property
    def passed(self) -> bool:
        return self.lake_only == self.postgres_only == 0


class Copy(NamedTuple):
    """A gold_internal table read from PostgreSQL, with the Iceberg snapshot it holds."""

    snapshot_id: int | None
    rows: DataFrame


class Violations(NamedTuple):
    rows: int
    violating: DataFrame


def aligned(rows: DataFrame, schema: StructType) -> DataFrame:
    """`rows` with the column names, order and types of `schema`."""
    return rows.select(*(F.col(field.name).cast(field.dataType) for field in schema.fields))


def differences(expected: DataFrame, actual: DataFrame) -> Differences:
    """The rows only one side has, comparing the two as multisets."""
    return Differences(expected.exceptAll(actual), actual.exceptAll(expected))


def plain(value: object) -> object:
    """`value` as a JSON report can hold it."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def sample(rows: DataFrame, columns: Sequence[str], size: int) -> Sample:
    """Up to `size` distinct values of `columns`, in their order."""
    picked = rows.select(*columns).distinct().orderBy(*columns).limit(size).collect()
    return tuple({name: plain(row[name]) for name in columns} for row in picked)


def compare(
    lake: DataFrame,
    postgres: DataFrame,
    columns: Sequence[str] | None = None,
    sample_size: int = SAMPLE_SIZE,
) -> Matching:
    """The rows only the Iceberg table or only its PostgreSQL copy has, as multisets, with
    samples of their `columns` (every column by default)."""
    postgres = aligned(postgres, lake.schema)
    lake_only, postgres_only = differences(lake, postgres)
    shown = list(columns or lake.columns)
    return Matching(
        lake_rows=lake.count(),
        postgres_rows=postgres.count(),
        lake_only=lake_only.count(),
        postgres_only=postgres_only.count(),
        samples={
            "lake_only": sample(lake_only, shown, sample_size),
            "postgres_only": sample(postgres_only, shown, sample_size),
        },
    )


def materialized(rows: DataFrame) -> DataFrame:
    """The rows read now: a JDBC source reads the table again on every action otherwise."""
    return rows.localCheckpoint(eager=True)


def within(table: StoredTable, rows: DataFrame, start: datetime) -> DataFrame:
    """A history table's rows from `start`; another table's rows as they are."""
    return rows.where(F.col(CHANGED_AT) >= F.lit(start)) if table.history else rows


def multiple_current(rows: DataFrame, key: str, current: Column | None) -> Violations:
    """The keys with more than one current row; every row is current without `current`."""
    counted = rows if current is None else rows.where(current)
    violating = counted.groupBy(key).count().where(F.col("count") > 1).select(key)
    return Violations(rows.select(key).distinct().count(), violating)


def overlapping(rows: DataFrame, key: str, starts_at: str) -> Violations:
    """The versions not folded whose range overlaps the next version of their key: open while a
    later one exists, or ending after it starts."""
    versions = rows.where(~F.col(VersionColumn.IS_FOLDED))
    after = Window.partitionBy(key).orderBy(COMMIT_SEQUENCE)
    next_start = F.lead(starts_at).over(after)
    valid_to = F.col(VersionColumn.VALID_TO)
    flagged = versions.withColumn("__next_start", next_start).where(
        F.col("__next_start").isNotNull() & (valid_to.isNull() | (F.col("__next_start") < valid_to))
    )
    return Violations(versions.count(), flagged.select(key, COMMIT_SEQUENCE))


def unresolved_keys(
    rows: DataFrame, fact: ChangeFact, seeded: Mapping[str, DataFrame], identity: Sequence[str]
) -> DataFrame:
    """The rows whose status or date key names no row of its seeded dimension, or is null
    while the lookup has a fallback, by `identity`."""
    misses: list[DataFrame] = []
    for lookup in fact.lookups:
        dimension = lookup.dimension.name
        if lookup.column not in rows.columns or SEED_KEYS.get(dimension) != lookup.key:
            continue
        known = seeded[dimension].select(F.col(lookup.key).alias("__known")).distinct()
        joined = rows.join(F.broadcast(known), F.col(lookup.column) == F.col("__known"), "left")
        missing = F.col("__known").isNull()
        if lookup.fallback is None:
            missing = missing & F.col(lookup.column).isNotNull()
        misses.append(joined.where(missing).select(*identity))
    if not misses:
        return rows.select(*identity).limit(0)
    union = misses[0]
    for miss in misses[1:]:
        union = union.unionByName(miss)
    return union


def unresolved_as_of(
    history: DataFrame, fact: ChangeFact, dimensions: Mapping[str, DataFrame]
) -> DataFrame:
    """The history rows whose as-of columns differ from the dimension version the model
    resolves for them, by key and `_commit_sequence`."""
    identity = [fact.key, COMMIT_SEQUENCE]
    misses: list[DataFrame] = []
    for lookup in fact.as_of:
        versions = dimensions[lookup.dimension.name]
        columns = lookup.fact_columns
        expected = with_as_of_lookup(history, fact.key, lookup, versions).select(
            *identity, *(F.col(column).alias(f"__expected_{column}") for column in columns)
        )
        differs = any_of(
            [~F.col(column).eqNullSafe(F.col(f"__expected_{column}")) for column in columns]
        )
        misses.append(history.join(expected, identity).where(differs).select(*identity))
    union = misses[0] if misses else history.select(*identity).limit(0)
    for miss in misses[1:]:
        union = union.unionByName(miss)
    return union


def recounted_reports(lake: Mapping[str, DataFrame]) -> dict[str, DataFrame]:
    """Every report table recounted from the current-state tables, as the gold model counts a
    batch's touched keys, with every key touched."""
    orders, items = lake["current_orders"], lake["current_order_items"]

    def kept(rows: DataFrame) -> DataFrame:
        return rows.where(~F.col(IS_REMOVED)).drop(IS_REMOVED)

    by_date_status = kept(
        open_orders_by_delivery_date_and_status(orders, orders, lake["dim_order_status"])
    )
    by_customer = kept(pending_orders_by_customer(orders, orders))
    return {
        OPEN_ORDERS_BY_DELIVERY_DATE_AND_STATUS: by_date_status,
        PENDING_ORDERS_BY_CUSTOMER: by_customer,
        PENDING_ITEMS_BY_PRODUCT: kept(
            pending_items_by_product(products_of_orders(orders, items), items, orders)
        ),
        TOP_DELIVERY_DATES: top_delivery_dates_by_open_orders(by_date_status),
        TOP_CUSTOMERS: top_customers_by_pending_orders(by_customer, lake["dim_customer"]),
    }


def current_rows_of(
    frames: Mapping[str, DataFrame], *, name: str, key: str, current: Column | None
) -> Violations:
    return multiple_current(frames[name], key, current)


def overlapping_in(
    frames: Mapping[str, DataFrame], *, name: str, key: str, starts_at: str
) -> Violations:
    return overlapping(frames[name], key, starts_at)


def unresolved_in(
    frames: Mapping[str, DataFrame], *, fact: ChangeFact, name: str, as_of: bool
) -> Violations:
    """The rows of `name` with a status or date key that names no seeded row, or for a history
    table with as-of columns the model would resolve otherwise."""
    rows = frames[name]
    identity = [fact.key, COMMIT_SEQUENCE]
    violating = unresolved_keys(rows, fact, frames, identity)
    if as_of:
        violating = violating.unionByName(unresolved_as_of(rows, fact, frames))
    return Violations(rows.count(), violating.distinct())


def recount_differences(frames: Mapping[str, DataFrame], *, table: StoredTable) -> Violations:
    """The rows only the stored report or only its recount has, each marked with its side."""
    stored = frames[table.name].select(*table.columns)
    recounted = aligned(recounted_reports(frames)[table.name], stored.schema)
    stored_only, recounted_only = differences(stored, recounted)
    marked = stored_only.withColumn("side", F.lit("stored")).unionByName(
        recounted_only.withColumn("side", F.lit("recounted"))
    )
    return Violations(stored.count(), marked)


def utc_today() -> date:
    return datetime.now(UTC).date()


class GoldCheck:
    """Checks the PostgreSQL copy of gold against the Iceberg tables, then the gold model's
    invariants on the Iceberg tables. Both read each table at the snapshot the copy holds, so
    the invariants check the state PostgreSQL serves and every table is read at a state the
    gold job wrote whole."""

    def __init__(
        self,
        spark: SparkSession,
        settings: GoldSettings,
        readers: GoldReaders,
        *,
        tables: Mapping[str, StoredTable] = STORED,
        dimensions: Sequence[Scd2Dimension] = DIMENSIONS,
        facts: Sequence[ChangeFact] = FACTS,
        today: Today = utc_today,
        sample_size: int = SAMPLE_SIZE,
    ) -> None:
        self.spark = spark
        self.settings = settings
        self.readers = readers
        self.tables = tables
        self.dimensions = tuple(dimensions)
        self.facts = tuple(facts)
        self.today = today
        self.sample_size = sample_size

    def subject(self, name: str) -> str:
        return self.settings.stored_table(name)

    def synced(self) -> dict[str, int | None]:
        """The Iceberg snapshot each gold_internal table holds, by table."""
        rows = self.readers.read_postgres(self.spark, SYNC_STATE).collect()
        return {
            str(row["table_name"]): None if row["snapshot_id"] is None else int(row["snapshot_id"])
            for row in rows
        }

    def copies(self) -> tuple[dict[str, Copy], list[str]]:
        """Every gold_internal table copied so far, with its snapshot, and the tables the gold
        job copied into again while they were read, `MAX_READS` times in a row.

        A copy changes a table and its sync_state row in one transaction, so a table whose
        sync_state row is the same before and after reading it was read whole at that snapshot.
        """
        before = self.synced()
        pending = [name for name in self.tables if name in before]
        copies: dict[str, Copy] = {}
        for _ in range(MAX_READS):
            relations = {name: self.tables[name].relation for name in pending}
            read = {
                name: materialized(self.readers.read_postgres(self.spark, relation))
                for name, relation in relations.items()
            }
            after = self.synced()
            for name, rows in read.items():
                if name in after and after[name] == before[name]:
                    copies[name] = Copy(after[name], rows)
            pending = [name for name in pending if name not in copies and name in after]
            before = after
            if not pending:
                break
        return copies, pending

    def _lake(self, name: str, copy: Copy | None) -> DataFrame | None:
        if copy is None:
            return None
        if copy.snapshot_id is None:
            return self.readers.lake.read(name).limit(0)
        return self.readers.lake.read_snapshot(name, copy.snapshot_id)

    def run(self) -> list[CheckResult]:
        copies, changing = self.copies()
        lake = {name: self._lake(name, copies.get(name)) for name in self.tables}
        start = retained_from(self.settings.retention_months, self.today())
        results = [
            self._parity(name, copies.get(name), lake[name], start, changing=name in changing)
            for name in self.tables
        ]
        results += self._one_current(lake)
        results += self._overlaps(lake)
        results += self._lookups(lake)
        results += self._recounts(lake)
        return results

    def _parity(
        self,
        name: str,
        copy: Copy | None,
        lake: DataFrame | None,
        start: datetime,
        *,
        changing: bool,
    ) -> CheckResult:
        table = self.tables[name]
        if copy is None:
            reason = (
                f"the gold job copied into it again during each of {MAX_READS} reads"
                if changing
                else "gold_internal.sync_state records no copy of it yet"
            )
            return self._not_checked(COPY_MATCHES_ICEBERG, name, PARITY_METRICS, reason)
        if lake is None:
            reason = f"its Iceberg table no longer has snapshot {copy.snapshot_id}"
            return self._not_checked(COPY_MATCHES_ICEBERG, name, PARITY_METRICS, reason)
        columns = list(table.columns)
        found = compare(
            within(table, lake.select(*columns), start),
            within(table, copy.rows.select(*columns), start),
            table.postgres_key or columns,
            self.sample_size,
        )
        details: dict[str, object] = {"snapshot_id": copy.snapshot_id, **found.samples}
        if table.history:
            details["history_from"] = start.isoformat()
        return COPY_MATCHES_ICEBERG.result(
            self.subject(name),
            passed=found.passed,
            metrics={metric: getattr(found, metric) for metric in PARITY_METRICS},
            details=details,
        )

    def _not_checked(
        self, check: Check, name: str, metrics: Sequence[str], reason: str
    ) -> CheckResult:
        return check.result(
            self.subject(name),
            passed=False,
            metrics=dict.fromkeys(metrics),
            details={"not_checked": reason},
        )

    def _invariant(
        self,
        check: Check,
        name: str,
        needed: Frames,
        compute: Callable[[Mapping[str, DataFrame]], Violations],
    ) -> CheckResult:
        unread = sorted(table for table, rows in needed.items() if rows is None)
        if unread:
            reason = f"no readable copied snapshot of {', '.join(unread)}"
            return self._not_checked(check, name, INVARIANT_METRICS, reason)
        frames = {table: rows for table, rows in needed.items() if rows is not None}
        result = compute(frames)
        columns = result.violating.columns
        violations = result.violating.count()
        return check.result(
            self.subject(name),
            passed=violations == 0,
            metrics={"rows": result.rows, "violations": violations},
            details={"violating": sample(result.violating, columns, self.sample_size)},
        )

    def _one_current(self, lake: Frames) -> list[CheckResult]:
        current = F.col(VersionColumn.IS_CURRENT)
        checked: list[tuple[str, str, Column | None]] = [
            (dimension.name, dimension.key, current) for dimension in self.dimensions
        ]
        checked += [(fact.history.name, fact.key, current) for fact in self.facts]
        checked += [(fact.current.name, fact.key, None) for fact in self.facts]
        return [
            self._invariant(
                ONE_CURRENT_VERSION,
                name,
                {name: lake[name]},
                partial(current_rows_of, name=name, key=key, current=flag),
            )
            for name, key, flag in checked
        ]

    def _overlaps(self, lake: Frames) -> list[CheckResult]:
        checked = [(dimension.name, dimension.key, VALID_FROM) for dimension in self.dimensions]
        checked += [(fact.history.name, fact.key, CHANGED_AT) for fact in self.facts]
        return [
            self._invariant(
                NO_OVERLAPPING_RANGES,
                name,
                {name: lake[name]},
                partial(overlapping_in, name=name, key=key, starts_at=start),
            )
            for name, key, start in checked
        ]

    def _lookups(self, lake: Frames) -> list[CheckResult]:
        results: list[CheckResult] = []
        seeded = {name: lake[name] for name in SEED_KEYS}
        for fact in self.facts:
            versions = {lookup.dimension.name: lake[lookup.dimension.name] for lookup in fact.as_of}
            for table, as_of in ((fact.history, True), (fact.current, False)):
                needed = {table.name: lake[table.name], **seeded, **(versions if as_of else {})}
                compute = partial(unresolved_in, fact=fact, name=table.name, as_of=as_of)
                results.append(self._invariant(RESOLVED_LOOKUPS, table.name, needed, compute))
        return results

    def _recounts(self, lake: Frames) -> list[CheckResult]:
        sources = {name: lake[name] for name in RECOUNT_SOURCES}
        return [
            self._invariant(
                REPORTS_MATCH_RECOUNT,
                name,
                {name: lake[name], **sources},
                partial(recount_differences, table=self.tables[name]),
            )
            for name in REPORTS
        ]


def check_gold(
    spark: SparkSession, settings: GoldSettings, readers: GoldReaders
) -> list[CheckResult]:
    return GoldCheck(spark, settings, readers).run()


def postgres_reader(settings: AnalyticsDbSettings) -> PostgresReader:
    """Reads a PostgreSQL relation through Spark's JDBC source."""

    def read(spark: SparkSession, relation: str) -> DataFrame:
        return spark.read.format("jdbc").options(**jdbc_options(settings, relation)).load()

    return read


def gold_readers(spark: SparkSession, settings: GoldSettings) -> GoldReaders:
    return GoldReaders(
        read_postgres=postgres_reader(settings.analytics_db),
        lake=iceberg_gold(spark, settings),
    )


def run_checks(spark: SparkSession, settings: GoldSettings) -> list[CheckResult]:
    return check_gold(spark, settings, gold_readers(spark, settings))
