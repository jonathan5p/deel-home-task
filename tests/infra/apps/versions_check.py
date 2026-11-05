import json
import platform

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("VersionsCheck").getOrCreate()

    def executor_python(_: int) -> str:
        return platform.python_version()

    executors = sorted(
        set(spark.sparkContext.parallelize(range(8), 8).map(executor_python).collect())
    )
    report = {
        "spark": spark.version,
        "driver_python": platform.python_version(),
        "executor_pythons": executors,
    }
    print(f"VERSIONS {json.dumps(report)}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
