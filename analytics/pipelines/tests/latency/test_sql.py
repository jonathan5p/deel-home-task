import pytest

from pipelines.latency.sql import (
    duckdb_path,
    identifier,
    integer,
    literal,
    relation,
    single_integer,
)


def test_literals_and_identifiers_double_their_quotes() -> None:
    assert literal("it's") == "'it''s'"
    assert identifier('a"b') == '"a""b"'
    assert relation("schema", "table") == '"schema"."table"'


def test_hadoop_s3_paths_become_duckdb_s3_paths() -> None:
    assert duckdb_path("s3a://lake/landing") == "s3://lake/landing"
    assert duckdb_path("/tmp/s3a://x") == "/tmp/s3a://x"


@pytest.mark.parametrize("value", [True, "1", 1.0, None])
def test_only_integers_are_read_back_as_integers(value: object) -> None:
    with pytest.raises(TypeError, match="expected an integer"):
        integer(value)


def test_a_single_integer_is_the_only_value_of_the_only_row() -> None:
    assert single_integer([(3,)]) == 3
    with pytest.raises(ValueError, match="unpack"):
        single_integer([(1,), (2,)])
