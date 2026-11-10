from tests.infra.stack import Stack, wait_until

TOPIC = "persistence-check"


def test_topics_and_records_survive_new_kafka_and_zookeeper_containers(stack: Stack) -> None:
    stack.kafka("kafka-topics", "--create", "--topic", TOPIC, "--partitions", "1")
    stack.kafka("kafka-console-producer", "--topic", TOPIC, stdin="kept\n")

    stack.recreate("zookeeper", "kafka")
    wait_until(stack.kafka_ready, timeout=120, message="Kafka never came back")

    records = stack.kafka(
        "kafka-console-consumer",
        "--topic",
        TOPIC,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "30000",
        check=False,
    )
    assert records.stdout.split() == ["kept"], records.stderr
