import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Self, TypedDict

REPO_ROOT = Path(__file__).resolve().parents[2]


def project_name(prefix: str, folder: str) -> str:
    """Name a Compose project after a checkout folder, with the same rule as the Makefile."""
    return prefix + re.sub(r"[^a-z0-9_-]", "-", folder.lower())


SHARED_GROUP = "shared-scenarios"
GOLD_SCHEMA_GROUP = "gold-schema"
# The modules that must run in one worker, and so on one stack, in file order. The users of the
# session scenarios share them, so that each scenario runs once. `make gold-reset` rewrites the
# rendered gold DDL in the checkout, which test_gold_model reads, and recreates the gold schemas
# without the monthly partitions that the rebuild's retention adds and test_gold_retention must
# not find.
XDIST_GROUPS: Mapping[str, str] = {
    "test_bronze.py": SHARED_GROUP,
    "test_gold_lake.py": SHARED_GROUP,
    "test_lake_catalog.py": SHARED_GROUP,
    "test_landing_inspection.py": SHARED_GROUP,
    "test_silver.py": SHARED_GROUP,
    "test_gold_model.py": GOLD_SCHEMA_GROUP,
    "test_gold_rebuild.py": GOLD_SCHEMA_GROUP,
    "test_gold_reset.py": GOLD_SCHEMA_GROUP,
    "test_gold_retention.py": GOLD_SCHEMA_GROUP,
}


def worker_project(project: str, env: Mapping[str, str]) -> str:
    """The Compose project of this pytest-xdist worker, so parallel workers never share a stack;
    a run without workers keeps the checkout's project."""
    worker = env.get("PYTEST_XDIST_WORKER")
    return f"{project}-{worker}" if worker else project


def xdist_scope(nodeid: str) -> str:
    """The unit of work that one worker runs whole: the test's group, or else its module, so
    that module fixtures run once."""
    module = nodeid.split("::")[0]
    return XDIST_GROUPS.get(PurePosixPath(module).name, module)


def misplaced_scenario_users(
    module_fixtures: Mapping[str, set[str]], shared: Collection[str]
) -> list[str]:
    """The modules that use a fixture in `shared` outside the shared group, where it would run
    a second time."""
    return sorted(
        module
        for module, fixtures in module_fixtures.items()
        if fixtures & set(shared) and xdist_scope(module) != SHARED_GROUP
    )


PROJECT = project_name("deel-infra-tests-", REPO_ROOT.name)
IMAGES = ("spark", "spark-app", "api-dev", "api", "iceberg-rest")
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
CATALOG_SERVICE = "iceberg-rest"


class MasterApp(TypedDict):
    id: str
    name: str
    state: str
    cores: int


@dataclass
class Stack:
    env: Mapping[str, str]
    running: list[subprocess.Popen[str]] = field(default_factory=list)
    catalog_started: bool = False
    python_started: bool = False

    @classmethod
    def isolated(cls, env: Mapping[str, str]) -> Self:
        """A stack in this worker's own Compose project. The session builds the images up front,
        so the drivers skip `make spark-submit`'s build check (`DRIVER_BUILD`)."""
        return cls(
            env={
                **env,
                "COMPOSE_PROJECT_NAME": worker_project(PROJECT, env),
                "COMPOSE_FILE": os.pathsep.join(COMPOSE_FILES),
                "DRIVER_BUILD": "",
            }
        )

    @property
    def project(self) -> str:
        return self.env["COMPOSE_PROJECT_NAME"]

    @property
    def python_container(self) -> str:
        return f"{self.project}-python"

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
        self.compose("--profile", "app", "build", "spark-app", "api-dev")
        self.compose("up", "--detach", "--build", "--wait", *CLUSTER_SERVICES)
        wait_until(self.kafka_ready, timeout=120, message="Kafka never accepted connections")

    def start_catalog(self) -> None:
        """Start the optional REST catalog once; later calls find it running."""
        if not self.catalog_started:
            self.compose(
                "--profile", "catalog", "up", "--detach", "--build", "--wait", CATALOG_SERVICE
            )
            self.catalog_started = True

    def python(self, script: str, *args: str) -> subprocess.CompletedProcess[str]:
        """Run a script with the app image's Python on the stack's network, in a container that
        the first call starts and later calls reuse, so a read costs no container or JVM start."""
        if not self.python_started:
            self.compose(
                "run",
                "--detach",
                "--no-deps",
                "--name",
                self.python_container,
                "spark-app",
                "sleep",
                "infinity",
            )
            self.python_started = True
        return subprocess.run(
            ["docker", "exec", self.python_container, "python", script, *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def kafka_ready(self) -> bool:
        return self.kafka("kafka-topics", "--list", check=False).returncode == 0

    def kafka(
        self, tool: str, *args: str, stdin: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        """Run a Kafka command-line tool inside the broker container against the broker."""
        command = ["exec", "-T", "kafka", tool, "--bootstrap-server", "kafka:29092", *args]
        return subprocess.run(
            ["docker", "compose", *command],
            cwd=REPO_ROOT,
            env=self.env,
            input=stdin,
            capture_output=True,
            text=True,
            check=check,
        )

    def recreate(self, *services: str) -> None:
        """Replace the services' containers with new ones, as `make down` and `make up` do."""
        self.compose("stop", *services)
        self.compose("rm", "--force", *services)
        self.compose("up", "--detach", "--wait", *services)

    def down(self) -> None:
        for process in self.running:
            if process.poll() is None:
                process.kill()
        for name in [*self.driver_containers(), self.python_container]:
            subprocess.run(["docker", "rm", "--force", name], capture_output=True, check=False)
        self.compose("--profile", "*", "down", "--volumes", "--remove-orphans", check=False)
        # Only the images this project builds: --rmi all would also delete the pulled ones.
        images = [f"{self.project}-{image}" for image in IMAGES]
        subprocess.run(
            ["docker", "image", "rm", "--force", *images], capture_output=True, check=False
        )

    def start_app(
        self,
        app: str,
        *args: str,
        conf: Mapping[str, str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        """Submit an app; `env` overrides the settings Compose passes to the driver container."""
        command = ["make", "--no-print-directory", "spark-submit", f"SPARK_APP={app}"]
        command.append("SPARK_UI_PORT=0")
        if args:
            command.append(f"SPARK_APP_ARGS={' '.join(args)}")
        if conf:
            command.append(f"SPARK_CONF={' '.join(f'{k}={v}' for k, v in conf.items())}")
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env={**self.env, **(env or {})},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.running.append(process)
        return process

    def finish(self, process: subprocess.Popen[str], timeout: float = 300) -> tuple[int, str]:
        output, _ = process.communicate(timeout=timeout)
        return process.returncode, output

    def run_app(
        self,
        app: str,
        *args: str,
        env: Mapping[str, str] | None = None,
        timeout: float = 300,
    ) -> tuple[int, str]:
        return self.finish(self.start_app(app, *args, env=env), timeout=timeout)

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
        """The driver containers of the app runs, which the kept Python container is not."""
        names = subprocess.run(
            [
                "docker",
                "ps",
                "--all",
                "--format",
                "{{.Names}}",
                "--filter",
                f"label=com.docker.compose.project={self.project}",
                "--filter",
                "label=com.docker.compose.service=spark-app",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return [name for name in names.split() if name != self.python_container]

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
