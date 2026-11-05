from pyspark.sql import SparkSession

TABLE = "lake.smoke.iceberg_check"


def main() -> None:
    spark = SparkSession.builder.appName("IcebergSmokeCheck").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.smoke")
    expected = spark.createDataFrame(
        [(1, "landing"), (2, "bronze"), (3, "silver"), (4, "gold")],
        "id INT, layer STRING",
    )
    expected.writeTo(TABLE).using("iceberg").createOrReplace()

    actual = spark.table(TABLE)
    if expected.exceptAll(actual).count() or actual.exceptAll(expected).count():
        spark.stop()
        raise SystemExit(f"Iceberg smoke check failed: {TABLE} does not match the rows written.")

    actual.orderBy("id").show()
    for row in spark.sql(f"SELECT file_path FROM {TABLE}.files").collect():
        print(f"data file: {row.file_path}")
    print(f"Iceberg smoke check passed: {TABLE} has {actual.count()} rows.")
    spark.stop()


if __name__ == "__main__":
    main()
