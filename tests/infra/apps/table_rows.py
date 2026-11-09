import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PREFIX = "TABLE ROWS "


def main(tables: list[str]) -> None:
    """Print every row of each table as one JSON line, with every value rendered as text."""
    spark = (
        SparkSession.builder.appName("table-rows")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    for table in tables:
        frame = spark.table(table)
        text = frame.select(
            *(F.col(column).cast("string").alias(column) for column in frame.columns)
        )
        rows = [row.asDict() for row in text.collect()]
        print(f"{PREFIX}{json.dumps({'table': table, 'rows': rows})}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
