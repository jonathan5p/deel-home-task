import json
import sys

from pyspark.sql import SparkSession

PREFIX = "TABLE STATS "


def main(table: str) -> None:
    """Print the table's data file count, snapshot operations and row ids as one JSON line."""
    spark = SparkSession.builder.appName("table-stats").getOrCreate()
    stats = {
        "data_files": spark.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0],
        "snapshots": [
            row.operation
            for row in spark.sql(
                f"SELECT operation FROM {table}.snapshots ORDER BY committed_at"
            ).collect()
        ],
        "ids": [row.id for row in spark.table(table).orderBy("id").collect()],
    }
    print(f"{PREFIX}{json.dumps(stats)}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
