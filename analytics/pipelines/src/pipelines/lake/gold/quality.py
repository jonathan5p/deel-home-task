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
from pipelines.lake.gold.model.columns import VersionColumn
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, VALID_FROM
from pipelines.lake.gold.public import PUBLIC_SCHEMA, PUBLIC_VIEWS, PublicView, view_query
from pipelines.lake.gold.retention import retained_from
from pipelines.lake.gold.scd2 import Scd2Dimension
from pipelines.lake.gold.serving import SYNC_STATE
from pipelines.lake.gold.sink import jdbc_options
from pipelines.lake.gold.stored import SEED_KEYS, StoredTable
from pipelines.quality.checks import Check, CheckResult
from pipelines.schemas.gold_definitions import GoldTable, load_gold_tables
from pipelines.settings import GoldSettings

SAMPLE_SIZE = 10
MAX_READS = 3
PARITY_METRICS = ("lake_rows", "postgres_rows", "lake_only", "postgres_only")
INVARIANT_METRICS = ("rows", "violations")
CHANGED_AT = str(FactColumn.CHANGED_AT)
REPORTS = (
    "open_orders_by_delivery_date_and_status",
    "top_delivery_dates_by_open_orders",
    "pending_items_by_product",
    "top_customers_by_pending_orders",
)
REPORT_SOURCES = ("current_orders", "current_order_items", "dim_order_status", "dim_customer")
GLOBAL_TEMP = "spark.sql.globalTempDatabase"
BUSINESS_KEYS: Mapping[str, tuple[str, str]] = {
    "fact_order_history": ("customer_id", "dim_customer"),
    "fact_order_item_history": ("product_id", "dim_product"),
}

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
    " allows it (an unknown status takes the UNKNOWN row).",
)
KNOWN_BUSINESS_KEYS = Check(
    "gold_business_keys_known",
    "Every history fact's dimension business key (an order's customer_id, an item's product_id)"
    " names a key the dimension holds, for every change committed no later than the"
    " dimension's newest version, which the gold views join as of the change.",
)
REPORTS_MATCH_LAKE = Check(
    "gold_report_views_match_lake",
    "Each PostgreSQL business-question view returns the rows the same view returns in the lake"
    " over the Iceberg tables at the snapshots the copy held when PostgreSQL answered it.",
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


def current_rows_of(
    frames: Mapping[str, DataFrame], *, name: str, key: str, current: Column | None
) -> Violations:
    return multiple_current(frames[name], key, current)


def overlapping_in(
    frames: Mapping[str, DataFrame], *, name: str, key: str, starts_at: str
) -> Violations:
    return overlapping(frames[name], key, starts_at)


def unresolved_in(frames: Mapping[str, DataFrame], *, fact: ChangeFact, name: str) -> Violations:
    """The rows of `name` with a status or date key that names no seeded row."""
    rows = frames[name]
    identity = [fact.key, COMMIT_SEQUENCE] if COMMIT_SEQUENCE in rows.columns else [fact.key]
    return Violations(rows.count(), unresolved_keys(rows, fact, frames, identity).distinct())


def unknown_business_keys(
    frames: Mapping[str, DataFrame],
    *,
    fact: ChangeFact,
    business_key: str,
    dimension: Scd2Dimension,
) -> Violations:
    """The history rows whose `business_key` names no key of the dimension, among the changes
    committed no later than the dimension's newest version. A source row exists before the
    changes that refer to it, so the dimension holds every key such a change names; a later
    change may still be waiting for its dimension's stream, which writes on its own."""
    history = frames[fact.history.name].where(F.col(business_key).isNotNull())
    versions = frames[dimension.name]
    newest = versions.agg(F.max(VALID_FROM).alias("__newest"))
    known = versions.select(F.col(dimension.key).alias("__known")).distinct()
    violating = (
        history.join(known, F.col(business_key) == F.col("__known"), "left_anti")
        .join(F.broadcast(newest), F.col(CHANGED_AT) <= F.col("__newest"))
        .select(fact.key, COMMIT_SEQUENCE, business_key)
    )
    return Violations(history.count(), violating)


def report_view_differences(
    frames: Mapping[str, DataFrame], *, view: PublicView, tables: Mapping[str, GoldTable]
) -> Violations:
    """The rows only PostgreSQL's answer or only the lake's answer to a business-question view
    has, each marked with its side. The lake's answer runs the view's query over the Iceberg
    tables at the snapshots PostgreSQL held, as global temporary views."""
    spark = frames[REPORT_SOURCES[0]].sparkSession
    for name in REPORT_SOURCES:
        frames[name].createOrReplaceGlobalTempView(name)
    namespace = spark.conf.get(GLOBAL_TEMP, "global_temp")
    lake = spark.sql(view_query(view, tables, str(namespace)))
    postgres = aligned(frames[f"{PUBLIC_SCHEMA}.{view.name}"], lake.schema)
    lake_only, postgres_only = differences(lake, postgres)
    marked = lake_only.withColumn("side", F.lit("lake")).unionByName(
        postgres_only.withColumn("side", F.lit("postgres"))
    )
    return Violations(lake.count(), marked)


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
        return None if copy is None else self._lake_at(name, copy.snapshot_id)

    def _lake_at(self, name: str, snapshot_id: int | None) -> DataFrame | None:
        """The Iceberg table at a snapshot, empty for none, or None once the snapshot is gone."""
        if snapshot_id is None:
            return self.readers.lake.read(name).limit(0)
        return self.readers.lake.read_snapshot(name, snapshot_id)

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
        results += self._business_keys(lake)
        results += self._report_views()
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
        self,
        check: Check,
        name: str,
        metrics: Sequence[str],
        reason: str,
        subject: str | None = None,
    ) -> CheckResult:
        return check.result(
            subject or self.subject(name),
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
        subject: str | None = None,
    ) -> CheckResult:
        unread = sorted(table for table, rows in needed.items() if rows is None)
        if unread:
            reason = f"no readable copied snapshot of {', '.join(unread)}"
            return self._not_checked(check, name, INVARIANT_METRICS, reason, subject)
        frames = {table: rows for table, rows in needed.items() if rows is not None}
        result = compute(frames)
        columns = result.violating.columns
        violations = result.violating.count()
        return check.result(
            subject or self.subject(name),
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
            for table in (fact.history, fact.current):
                needed = {table.name: lake[table.name], **seeded}
                compute = partial(unresolved_in, fact=fact, name=table.name)
                results.append(self._invariant(RESOLVED_LOOKUPS, table.name, needed, compute))
        return results

    def _business_keys(self, lake: Frames) -> list[CheckResult]:
        dimensions = {dimension.name: dimension for dimension in self.dimensions}
        results: list[CheckResult] = []
        for fact in self.facts:
            business_key, name = BUSINESS_KEYS[fact.name]
            dimension = dimensions[name]
            needed = {fact.history.name: lake[fact.history.name], name: lake[name]}
            compute = partial(
                unknown_business_keys, fact=fact, business_key=business_key, dimension=dimension
            )
            results.append(self._invariant(KNOWN_BUSINESS_KEYS, fact.history.name, needed, compute))
        return results

    def answers(self) -> tuple[dict[str, DataFrame], dict[str, int | None]] | None:
        """PostgreSQL's answer to every business-question view, and the snapshots of the tables
        they read, or None when the gold job copied into those tables during each of
        `MAX_READS` reads. A copy changes a table and its sync_state row in one transaction, so
        answers read while the sync_state rows stayed the same were all read at those
        snapshots."""
        for _ in range(MAX_READS):
            before = self.synced()
            read = {
                name: materialized(
                    self.readers.read_postgres(self.spark, f"{PUBLIC_SCHEMA}.{name}")
                )
                for name in REPORTS
            }
            after = self.synced()
            if all(name in after and after[name] == before.get(name) for name in REPORT_SOURCES):
                return read, {name: after[name] for name in REPORT_SOURCES}
        return None

    def _report_views(self) -> list[CheckResult]:
        answered = self.answers()
        sources: dict[str, DataFrame | None] = dict.fromkeys(REPORT_SOURCES)
        answers: dict[str, DataFrame | None] = {}
        if answered is not None:
            read, snapshots = answered
            sources = {name: self._lake_at(name, snapshots[name]) for name in REPORT_SOURCES}
            answers = {f"{PUBLIC_SCHEMA}.{name}": rows for name, rows in read.items()}
        views = {view.name: view for view in PUBLIC_VIEWS}
        tables = load_gold_tables()
        return [
            self._invariant(
                REPORTS_MATCH_LAKE,
                name,
                {**sources, f"{PUBLIC_SCHEMA}.{name}": answers.get(f"{PUBLIC_SCHEMA}.{name}")},
                partial(report_view_differences, view=views[name], tables=tables),
                subject=f"{PUBLIC_SCHEMA}.{name}",
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
