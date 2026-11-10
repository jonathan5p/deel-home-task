import pytest
from jinja2 import UndefinedError

from pipelines.gold.sql import prefixed, render_sql


def test_a_template_is_rendered_without_a_trailing_newline() -> None:
    statement = render_sql("scd2_insert", relation="r", staging="s", staged=["a"], key="a")

    assert statement.endswith("ON CONFLICT (a, _commit_sequence) DO NOTHING")


def test_a_template_value_that_is_not_given_is_refused() -> None:
    with pytest.raises(UndefinedError, match="relation"):
        render_sql("scd2_insert", staging="s", staged=["a"], key="a")


def test_columns_are_prefixed_with_a_relation_alias_in_templates() -> None:
    assert prefixed(["a", "b"], "stored") == ["stored.a", "stored.b"]
