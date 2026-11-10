import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

type Metrics = Mapping[str, int | None]
type Details = Mapping[str, object]


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    description: str
    subject: str
    passed: bool
    metrics: Metrics
    details: Details = field(default_factory=dict)
    """Evidence beyond the counts, such as sample keys of mismatched rows; JSON-serializable."""


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    description: str

    def result(
        self, subject: str, *, passed: bool, metrics: Metrics, details: Details | None = None
    ) -> CheckResult:
        return CheckResult(
            check=self.name,
            description=self.description,
            subject=subject,
            passed=passed,
            metrics=metrics,
            details=details or {},
        )


def all_passed(results: Sequence[CheckResult]) -> bool:
    """A run that checked nothing has verified nothing, so it does not pass."""
    return bool(results) and all(result.passed for result in results)


def result_object(result: CheckResult) -> dict[str, object]:
    """The result as printed and reported; `details` only when the check gave any."""
    fields: dict[str, object] = {
        "check": result.check,
        "subject": result.subject,
        "passed": result.passed,
        "metrics": dict(result.metrics),
    }
    if result.details:
        fields["details"] = dict(result.details)
    return fields | {"description": result.description}


def summary_object(results: Sequence[CheckResult]) -> dict[str, object]:
    return {
        "summary": True,
        "checks": len(results),
        "failed": sum(not result.passed for result in results),
        "passed": all_passed(results),
    }


def report_lines(prefix: str, results: Sequence[CheckResult]) -> list[str]:
    lines = [f"{prefix} {json.dumps(result_object(result))}" for result in results]
    lines.append(f"{prefix} {json.dumps(summary_object(results))}")
    return lines
