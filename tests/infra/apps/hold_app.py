"""Spark application that keeps its executors for a given number of seconds, then exits."""

import sys
import time

from pyspark.sql import SparkSession

DEFAULT_SECONDS = 30


def main(argv: list[str]) -> None:
    seconds = int(argv[0]) if argv else DEFAULT_SECONDS
    spark = SparkSession.builder.getOrCreate()
    rows = spark.range(1_000).count()
    print(
        f"hold app {spark.sparkContext.appName}: counted {rows} rows, holding {seconds}s",
        flush=True,
    )
    time.sleep(seconds)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
