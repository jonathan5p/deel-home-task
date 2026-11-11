import json
import subprocess
from typing import Any
from urllib.parse import quote

from tests.infra.stack import REPO_ROOT, Stack

PROMETHEUS = "prometheus"
STAGE_EXPORTER = "spark-stage-exporter"
METRICS_PROFILE = ("--profile", "metrics")
RULE_TESTS = "tests/prometheus/spark_rules_test.yml"

# Prometheus's API answers with untyped JSON.
type Sample = dict[str, Any]


def start_prometheus(stack: Stack) -> None:
    stack.compose(*METRICS_PROFILE, "up", "--detach", "--wait", PROMETHEUS)


def remove_prometheus(stack: Stack) -> None:
    stack.compose(*METRICS_PROFILE, "rm", "--stop", "--force", PROMETHEUS)


def start_stage_exporter(stack: Stack) -> None:
    """The exporter and the History Server it reads; both need no host port."""
    stack.compose(*METRICS_PROFILE, "up", "--detach", "--wait", STAGE_EXPORTER)


def remove_stage_exporter(stack: Stack) -> None:
    stack.compose(*METRICS_PROFILE, "rm", "--stop", "--force", STAGE_EXPORTER, "spark-history")


def restart_prometheus(stack: Stack) -> None:
    """Replace the container with a new one on the same data volume."""
    remove_prometheus(stack)
    start_prometheus(stack)


def query(stack: Stack, promql: str) -> list[Sample]:
    """The instant vector Prometheus returns for a query, from inside its container."""
    run = stack.compose(
        "exec",
        "-T",
        PROMETHEUS,
        "wget",
        "-qO-",
        f"http://localhost:9090/api/v1/query?query={quote(promql)}",
        check=False,
    )
    assert run.returncode == 0, run.stderr
    answer = json.loads(run.stdout)
    assert answer["status"] == "success", answer
    return answer["data"]["result"]


def run_rule_tests(stack: Stack) -> subprocess.CompletedProcess[str]:
    """Run promtool's unit tests of the alert rules, in the Prometheus image."""
    return stack.compose(
        *METRICS_PROFILE,
        "run",
        "--rm",
        "--no-deps",
        "--volume",
        f"{REPO_ROOT / RULE_TESTS}:/tests/rules_test.yml:ro",
        "--entrypoint",
        "promtool",
        PROMETHEUS,
        "test",
        "rules",
        "/tests/rules_test.yml",
        check=False,
    )
