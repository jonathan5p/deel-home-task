from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning
from pyspark.sql.types import StructField, StructType

from pipelines.lake.gold.model.columns import IS_REMOVED
from pipelines.lake.gold.model.versions import COMMIT_SEQUENCE, all_of
from pipelines.lake.gold.stored import StoredTable, WriteMode
from pipelines.lake.shared.snapshots import read_snapshot
from pipelines.lake.shared.tables import (
    COMMENT,
    description_updates,
    sql_identifier,
    table_comment,
)
from pipelines.lake.shared.types import SPARK_TYPES
from pipelines.schemas.gold_definitions import GoldTable

SOURCE = "source"
HISTORY_BUCKETS = 16
TABLE_PROPERTIES: Mapping[str, str] = {
    "format-version": "2",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}
MAIN_BRANCH = "main"


def described_schema(table: GoldTable) -> StructType:
    """The Iceberg schema of a gold table, each column described. Every column is nullable, as
    in the lake's other tables, so a column's nullability can change without a rewrite."""
    return StructType(
        [
            StructField(
                column.name,
                SPARK_TYPES[column.type],
                nullable=True,
                metadata={COMMENT: column.description},
            )
            for column in table.columns
        ]
    )


def _source(column: str) -> Column:
    return F.col(f"{SOURCE}.{column}")


class IcebergGold:
    """The gold tables in the lake: one Iceberg table per stored gold table, named by
    `relation`, written with `mergeInto` as each table's mode says."""

    def __init__(
        self,
        spark: SparkSession,
        tables: Mapping[str, StoredTable],
        relation: Mapping[str, str],
    ) -> None:
        self.spark = spark
        self.tables = tables
        self.relation = relation

    def ensure(self) -> list[str]:
        """Create the missing tables, and bring the descriptions of the others up to date;
        returns the description updates applied."""
        updates: list[str] = []
        for table in self.tables.values():
            updates += self._ensure(table)
        return updates

    def _ensure(self, table: StoredTable) -> list[str]:
        relation = self.relation[table.name]
        schema = described_schema(table.table)
        if not self.spark.catalog.tableExists(relation):
            self._create(table, relation, schema)
            return []
        stored = self.spark.table(relation).schema
        comment = table_comment(self.spark, relation)
        updates = description_updates(relation, stored, comment, schema, table.table.description)
        for statement in updates:
            self.spark.sql(statement)
        return updates

    def _create(self, table: StoredTable, relation: str, schema: StructType) -> None:
        namespace = relation.rpartition(".")[0]
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        writer = self.spark.createDataFrame([], schema).writeTo(relation).using("iceberg")
        for name, value in TABLE_PROPERTIES.items():
            writer = writer.tableProperty(name, value)
        writer = writer.tableProperty(COMMENT, table.table.description)
        if table.history:
            writer = writer.partitionedBy(partitioning.bucket(HISTORY_BUCKETS, table.keys[0]))
        writer.create()
        if table.history:
            order = ", ".join(sql_identifier(key) for key in table.keys)
            self.spark.sql(f"ALTER TABLE {relation} WRITE LOCALLY ORDERED BY {order}")

    def drop(self) -> None:
        """Drop every gold table with its files."""
        for name in self.tables:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.relation[name]} PURGE")

    def read(self, name: str) -> DataFrame:
        return self.spark.table(self.relation[name])

    def read_snapshot(self, name: str, snapshot_id: int) -> DataFrame | None:
        """The table's rows at a snapshot, or None when the table no longer has it."""
        relation = self.relation[name]
        snapshots = self.spark.table(f"{relation}.snapshots")
        if snapshots.where(F.col("snapshot_id") == snapshot_id).isEmpty():
            return None
        return read_snapshot(self.spark, relation, snapshot_id)

    def snapshot(self, name: str) -> int | None:
        """The id of the table's current snapshot, or None when it has none."""
        refs = self.spark.table(f"{self.relation[name]}.refs")
        main = refs.where(F.col("name") == MAIN_BRANCH).select("snapshot_id").first()
        return None if main is None else int(main["snapshot_id"])

    def unchanged(self, table: StoredTable, rows: DataFrame) -> bool:
        """Whether writing `rows` would change nothing: there are none, or they replace equal
        rows."""
        if table.mode is not WriteMode.REPLACE:
            return rows.isEmpty()
        stored = self.read(table.name).select(*table.columns)
        new = rows.select(*table.columns)
        return new.exceptAll(stored).isEmpty() and stored.exceptAll(new).isEmpty()

    def write(self, table: StoredTable, rows: DataFrame) -> None:
        relation = self.relation[table.name]
        if table.mode is WriteMode.REPLACE:
            rows.select(*table.columns).writeTo(relation).overwrite(F.lit(True))
            return
        if table.mode is WriteMode.RECOUNT:
            self._recount(table, relation, rows)
            return
        matched = all_of([_source(key) == F.col(f"{relation}.{key}") for key in table.keys])
        merge = rows.select(*table.columns).alias(SOURCE).mergeInto(relation, matched)
        if table.mode is WriteMode.MERGE_NEWER:
            newer = _source(COMMIT_SEQUENCE) > F.col(f"{relation}.{COMMIT_SEQUENCE}")
            merge = merge.whenMatched(newer).updateAll()
        else:
            merge = merge.whenMatched().updateAll()
        merge.whenNotMatched().insertAll().merge()

    def _recount(self, table: StoredTable, relation: str, rows: DataFrame) -> None:
        keys = set(table.keys)
        matched = all_of(
            [_source(key).eqNullSafe(F.col(f"{relation}.{key}")) for key in table.keys]
        )
        totals = {column: _source(column) for column in table.columns if column not in keys}
        removed = _source(IS_REMOVED)
        (
            rows.select(*table.columns, IS_REMOVED)
            .alias(SOURCE)
            .mergeInto(relation, matched)
            .whenMatched(removed)
            .delete()
            .whenMatched()
            .update(totals)
            .whenNotMatched(~removed)
            .insert({column: _source(column) for column in table.columns})
            .merge()
        )
