from pipelines.quality.checks import Check, CheckResult

LAKE_ENV = {"LAKE_ROOT": "s3a://lake/", "ICEBERG_CATALOG": "lake"}
ROWS_LANDED = Check("rows_landed", "Every row landed.")


def result(subject: str, *, passed: bool) -> CheckResult:
    return ROWS_LANDED.result(subject, passed=passed, metrics={"missing": 0 if passed else 1})


class FakeWriter:
    """Keeps the written files in memory, keyed by path."""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def __call__(self, path: str, text: str) -> None:
        self.files[path] = text
