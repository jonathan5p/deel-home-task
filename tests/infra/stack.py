import json
import os
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self, TypedDict

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT = "deel-infra-tests"
COMPOSE_FILES = ("docker-compose.yaml", "tests/infra/compose.override.yaml")
CLUSTER_SERVICES = (
    "analytics-db",
    "seaweedfs",
    "zookeeper",
    "kafka",
    "spark-master",
    "spark-worker-1",
    "spark-worker-2",
)


class MasterApp(TypedDict):
    id: str
    name: str
    state: str
    cores: int


@dataclass
class Stack:
    env: Mapping[str, str]
    running: list[subprocess.Popen[str]] = field(default_factory=list)

    @classmethod
    def isolated(cls) -> Self:
        env = os.environ | {
            "COMPOSE_PROJECT_NAME": PROJECT,
            "COMPOSE_FILE": os.pathsep.join(COMPOSE_FILES),
        }
        return cls(env=env)

    def compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            check=check,
        )

    def up(self) -> None:
        self.compose("--profile", "app", "build", "spark-app")
        self.compose("up", "--detach", "--build", "--wait", *CLUSTER_SERVICES)
        wait_until(self.kafka_ready, timeout=120, message="Kafka never accepted connections")

    def kafka_ready(self) -> bool:
        listing = self.compose(
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "kafka:29092",
            "--list",
            check=False,
        )
        return listing.returncode == 0

    def down(self) -> None:
        for process in self.running:
            if process.poll() is None:
                process.kill()
        for name in self.driver_containers():
            subprocess.run(["docker", "rm", "--force", name], capture_output=True, check=False)
        self.compose("--profile", "*", "down", "--volumes", "--remove-orphans", check=False)

    def start_app(
        self, app: str, *args: str, conf: Mapping[str, str] | None = None
    ) -> subprocess.Popen[str]:
        command = ["make", "--no-print-directory", "spark-submit", f"SPARK_APP={app}"]
        command.append("SPARK_UI_PORT=0")
        if args:
            command.append(f"SPARK_APP_ARGS={' '.join(args)}")
        if conf:
            command.append(f"SPARK_CONF={' '.join(f'{k}={v}' for k, v in conf.items())}")
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.running.append(process)
        return process

    def finish(self, process: subprocess.Popen[str], timeout: float = 300) -> tuple[int, str]:
        output, _ = process.communicate(timeout=timeout)
        return process.returncode, output

    def run_app(self, app: str, *args: str, timeout: float = 300) -> tuple[int, str]:
        return self.finish(self.start_app(app, *args), timeout=timeout)

    def run_sql_file(
        self,
        sql_file: str,
        landing_root: str,
        variables: Mapping[str, str],
        timeout: float = 300,
    ) -> subprocess.CompletedProcess[str]:
        """Run a SQL file with make spark-sql; stdout holds only the query results."""
        return subprocess.run(
            [
                "make",
                "--no-print-directory",
                "spark-sql",
                f"SPARK_SQL_FILE={sql_file}",
                f"LANDING_ROOT={landing_root}",
                f"SPARK_SQL_VARS={' '.join(f'{k}={v}' for k, v in variables.items())}",
                "SPARK_UI_PORT=0",
            ],
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def active_apps(self) -> list[MasterApp]:
        state = self.compose(
            "exec", "-T", "spark-master", "curl", "-fsS", "http://localhost:8080/json/"
        ).stdout
        apps: list[MasterApp] = json.loads(state)["activeapps"]
        return apps

    def driver_containers(self) -> list[str]:
        names = subprocess.run(
            [
                "docker",
                "ps",
                "--all",
                "--format",
                "{{.Names}}",
                "--filter",
                f"label=com.docker.compose.project={PROJECT}",
                "--filter",
                "label=com.docker.compose.service=spark-app",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return names.split()

    def sql(self, database: str, query: str) -> str:
        return self.compose(
            "exec", "-T", "analytics-db", "psql", "-U", "analytics", "-d", database, "-Atc", query
        ).stdout.strip()

    def put_object(self, key: str, data: bytes) -> None:
        """Upload bytes to the lake bucket from inside the SeaweedFS container (no host port)."""
        upload = (
            'curl -fsS -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" '
            '--user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" '
            f'--data-binary @- "http://localhost:8333/lake/{key}"'
        )
        subprocess.run(
            ["docker", "compose", "exec", "-T", "seaweedfs", "sh", "-c", upload],
            cwd=REPO_ROOT,
            env=self.env,
            input=data,
            capture_output=True,
            check=True,
        )

    def lake_entries(self, path: str) -> set[str]:
        listing = self.compose(
            "exec",
            "-T",
            "seaweedfs",
            "curl",
            "-fsS",
            "-H",
            "Accept: application/json",
            f"http://localhost:8888/buckets/lake/{path}?limit=1000",
        ).stdout
        entries: list[dict[str, str]] = json.loads(listing)["Entries"] or []
        return {entry["FullPath"].rstrip("/").rsplit("/", 1)[-1] for entry in entries}


def wait_until(condition: Callable[[], bool], timeout: float, message: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(1)
    raise TimeoutError(message)
