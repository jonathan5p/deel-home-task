import json

from pyspark.sql import SparkSession

AVRO_PATH = "s3a://lake/infra-check/avro"
TOPIC = "infra-check"
ROWS = 100


def main() -> None:
    spark = SparkSession.builder.appName("ConnectorsCheck").getOrCreate()
    rows = spark.range(0, ROWS, 1, 4).selectExpr(
        "cast(id as string) as key", "concat('value-', cast(id as string)) as value"
    )

    rows.write.mode("overwrite").format("avro").save(AVRO_PATH)
    avro_rows = spark.read.format("avro").load(AVRO_PATH).count()

    rows.write.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option(
        "topic", TOPIC
    ).save()
    kafka_rows = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .count()
    )

    print(
        f"CONNECTORS {json.dumps({'avro_rows': avro_rows, 'kafka_rows': kafka_rows})}", flush=True
    )
    spark.stop()


if __name__ == "__main__":
    main()
