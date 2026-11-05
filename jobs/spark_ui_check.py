import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

DURATION_SECONDS = 60
TRIGGER_SECONDS = 5


def main() -> None:
    spark = (
        SparkSession.builder.appName("SparkUICheck")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 100)
        .load()
        .groupBy(F.window("timestamp", "10 seconds"))
        .count()
        .writeStream.format("noop")
        .outputMode("update")
        .queryName("rate_window_counts")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    deadline = time.monotonic() + DURATION_SECONDS
    try:
        while time.monotonic() < deadline:
            time.sleep(TRIGGER_SECONDS)
            progress = stream.lastProgress
            if progress:
                print(
                    f"batch {progress['batchId']}: "
                    f"{progress.get('inputRowsPerSecond', 0):.0f} rows/s in, "
                    f"{progress.get('processedRowsPerSecond', 0):.0f} rows/s processed",
                    flush=True,
                )
    finally:
        stop_between_micro_batches(stream)
        spark.stop()
    print("Spark UI check finished.", flush=True)


def stop_between_micro_batches(
    stream: StreamingQuery, timeout_seconds: float = 30.0
) -> None:
    """Stop the query while no micro-batch is running, so it is not aborted mid-write and logged as an error."""
    deadline = time.monotonic() + timeout_seconds
    while stream.status["isTriggerActive"] and time.monotonic() < deadline:
        time.sleep(0.2)
    stream.stop()


if __name__ == "__main__":
    main()
