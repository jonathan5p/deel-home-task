import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    subject: str
    passed: bool
    metrics: Mapping[str, int | None]


def all_passed(results: Iterable[CheckResult]) -> bool:
    return all(result.passed for result in results)


def report_lines(prefix: str, results: Sequence[CheckResult]) -> list[str]:
    lines = [
        f"{prefix} "
        + json.dumps(
            {
                "check": result.check,
                "subject": result.subject,
                "passed": result.passed,
                "metrics": dict(result.metrics),
            }
        )
        for result in results
    ]
    failed = sum(not result.passed for result in results)
    summary = {"summary": True, "checks": len(results), "failed": failed, "passed": failed == 0}
    lines.append(f"{prefix} {json.dumps(summary)}")
    return lines
