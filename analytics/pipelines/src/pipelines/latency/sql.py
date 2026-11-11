from collections.abc import Sequence

type Row = tuple[object, ...]


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def relation(*parts: str) -> str:
    return ".".join(identifier(part) for part in parts)


def duckdb_path(path: str) -> str:
    """DuckDB reads S3 through `s3://`; Spark's paths use Hadoop's `s3a://` scheme."""
    return "s3://" + path.removeprefix("s3a://") if path.startswith("s3a://") else path


def integer(value: object) -> int:
    """An integer read back from a query; booleans and other types are rejected."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an integer, got {value!r}")
    return value


def single_integer(rows: Sequence[Row]) -> int:
    (row,) = rows
    (value,) = row
    return integer(value)
