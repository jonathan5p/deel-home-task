import json

from pipelines.quality.checks import Check, CheckResult, all_passed, report_lines

UNIQUE = Check("unique_offsets", "Each offset landed once.")
PASSED = UNIQUE.result("topic-a[0]", passed=True, metrics={"rows": 3})
FAILED = UNIQUE.result("topic-a[1]", passed=False, metrics={"rows": 2})


def test_a_check_builds_results_that_carry_its_name_and_description() -> None:
    result = UNIQUE.result("topic-a[0]", passed=True, metrics={"rows": 3})

    assert result == CheckResult(
        check="unique_offsets",
        description="Each offset landed once.",
        subject="topic-a[0]",
        passed=True,
        metrics={"rows": 3},
    )


def test_all_passed_needs_every_result_to_pass() -> None:
    assert all_passed([PASSED])
    assert not all_passed([PASSED, FAILED])


def test_all_passed_fails_when_nothing_was_checked() -> None:
    assert not all_passed([])


def test_report_lines_are_json_with_a_prefix_and_a_summary() -> None:
    lines = report_lines("LANDING QUALITY", [PASSED, FAILED])

    assert [json.loads(line.removeprefix("LANDING QUALITY ")) for line in lines] == [
        {
            "check": "unique_offsets",
            "subject": "topic-a[0]",
            "passed": True,
            "metrics": {"rows": 3},
            "description": "Each offset landed once.",
        },
        {
            "check": "unique_offsets",
            "subject": "topic-a[1]",
            "passed": False,
            "metrics": {"rows": 2},
            "description": "Each offset landed once.",
        },
        {"summary": True, "checks": 2, "failed": 1, "passed": False},
    ]


def test_the_summary_of_an_empty_report_does_not_pass() -> None:
    (summary,) = report_lines("LANDING QUALITY", [])

    assert json.loads(summary.removeprefix("LANDING QUALITY ")) == {
        "summary": True,
        "checks": 0,
        "failed": 0,
        "passed": False,
    }
