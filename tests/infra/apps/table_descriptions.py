import json
import sys

from pyspark.sql import SparkSession

PREFIX = "TABLE DESCRIPTION "


def main(tables: list[str]) -> None:
    """Print each table's comment and column comments as one JSON line."""
    spark = SparkSession.builder.appName("table-descriptions").getOrCreate()
    for table in tables:
        details = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        comment = next((row.data_type for row in details if row.col_name == "Comment"), None)
        columns = {field.name: field.metadata.get("comment") for field in spark.table(table).schema}
        dump = {"table": table, "comment": comment, "columns": columns}
        print(f"{PREFIX}{json.dumps(dump)}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
