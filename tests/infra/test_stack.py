import pytest

from tests.infra.stack import project_name


@pytest.mark.parametrize(
    ("folder", "expected"),
    [
        ("deel_home_task", "prefix-deel_home_task"),
        ("feat-23-landing-check", "prefix-feat-23-landing-check"),
        ("Feat.23 Landing+Check", "prefix-feat-23-landing-check"),
    ],
)
def test_project_names_keep_only_compose_safe_characters(folder: str, expected: str) -> None:
    assert project_name("prefix-", folder) == expected
