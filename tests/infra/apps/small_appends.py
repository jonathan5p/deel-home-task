import sys

from pyspark.sql import SparkSession


def main(table: str, count: int) -> None:
    spark = SparkSession.builder.appName("small-appends").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    for row_id in range(count):
        rows = spark.createDataFrame([(row_id, "a")], "id INT, state STRING")
        if row_id == 0:
            rows.writeTo(table).using("iceberg").create()
        else:
            rows.writeTo(table).append()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
