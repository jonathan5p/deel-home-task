import json

from pipelines.quality.checks import CheckResult, all_passed, report_lines

PASSED = CheckResult(check="unique", subject="topic-a[0]", passed=True, metrics={"rows": 3})
FAILED = CheckResult(check="contiguous", subject="topic-a[0]", passed=False, metrics={"gaps": 1})


def test_all_passed_needs_every_result_to_pass() -> None:
    assert all_passed([PASSED])
    assert not all_passed([PASSED, FAILED])


def test_report_lines_are_json_with_a_prefix_and_a_summary() -> None:
    lines = report_lines("LANDING QUALITY", [PASSED, FAILED])

    assert [json.loads(line.removeprefix("LANDING QUALITY ")) for line in lines] == [
        {"check": "unique", "subject": "topic-a[0]", "passed": True, "metrics": {"rows": 3}},
        {"check": "contiguous", "subject": "topic-a[0]", "passed": False, "metrics": {"gaps": 1}},
        {"summary": True, "checks": 2, "failed": 1, "passed": False},
    ]
