import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

type Metrics = Mapping[str, int | None]


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    description: str
    subject: str
    passed: bool
    metrics: Metrics


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    description: str

    def result(self, subject: str, *, passed: bool, metrics: Metrics) -> CheckResult:
        return CheckResult(
            check=self.name,
            description=self.description,
            subject=subject,
            passed=passed,
            metrics=metrics,
        )


def all_passed(results: Sequence[CheckResult]) -> bool:
    """A run that checked nothing has verified nothing, so it does not pass."""
    return bool(results) and all(result.passed for result in results)


def report_lines(prefix: str, results: Sequence[CheckResult]) -> list[str]:
    lines = [
        f"{prefix} "
        + json.dumps(
            {
                "check": result.check,
                "subject": result.subject,
                "passed": result.passed,
                "metrics": dict(result.metrics),
                "description": result.description,
            }
        )
        for result in results
    ]
    failed = sum(not result.passed for result in results)
    summary = {
        "summary": True,
        "checks": len(results),
        "failed": failed,
        "passed": all_passed(results),
    }
    lines.append(f"{prefix} {json.dumps(summary)}")
    return lines
