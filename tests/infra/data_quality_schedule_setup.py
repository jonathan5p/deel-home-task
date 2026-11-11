import json

from tests.infra.stack import Stack, wait_until

SERVICE = "data-quality-job"
PROFILE = "data-quality"
SUMMARY_PREFIX = "DATA QUALITY "
FAILED_RUN = "Data quality checks failed"


def create_source_topics(stack: Stack) -> None:
    """Create the Kafka topics the service checks, which Debezium creates on the dev stack."""
    config = json.loads(
        stack.compose("--profile", PROFILE, "config", "--format", "json", SERVICE).stdout
    )
    topics = config["services"][SERVICE]["environment"]["KAFKA_TOPICS"].split(",")
    for topic in topics:
        stack.kafka("kafka-topics", "--create", "--if-not-exists", "--topic", topic)


def first_scheduled_run(stack: Stack) -> str:
    """The log of the scheduled data quality service's first run."""

    def log() -> str:
        return stack.compose("logs", "--no-log-prefix", SERVICE).stdout

    try:
        stack.compose("--profile", PROFILE, "up", "--detach", SERVICE)
        wait_until(
            lambda: SUMMARY_PREFIX in log() or FAILED_RUN in log(),
            timeout=600,
            message="the data quality service never finished a run",
        )
        return log()
    finally:
        stack.compose("--profile", PROFILE, "rm", "-sf", SERVICE, check=False)


def run_summary(log: str) -> dict[str, object]:
    lines = [line for line in log.splitlines() if line.startswith(SUMMARY_PREFIX)]
    assert len(lines) == 1, log
    summary: dict[str, object] = json.loads(lines[0].removeprefix(SUMMARY_PREFIX))
    return summary
