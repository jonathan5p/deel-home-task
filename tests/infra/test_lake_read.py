from datetime import datetime

import duckdb
import pytest

from tests.infra.apps.lake_read import description, relation, spark_text, table_rows

TYPED_ROW = """
SELECT
    7::BIGINT AS id,
    'ACME INC' AS name,
    true AS is_active,
    1.53::DECIMAL(38, 10) AS price,
    DATE '2026-11-08' AS day,
    TIMESTAMPTZ '2026-11-08 14:21:00.022+00' AS changed_at,
    TIMESTAMPTZ '2026-11-08 16:21:01+02' AS published_at,
    TIMESTAMP '2026-11-08 14:21:00.123456' AS loaded_at,
    'not json'::BLOB AS value,
    '\\xFF'::BLOB AS undecodable,
    NULL::TIMESTAMPTZ AS deleted_at,
    NULL::VARCHAR AS note
"""


def test_rows_read_as_the_text_spark_casts_each_type_to() -> None:
    connection = duckdb.connect()
    connection.execute(f"CREATE TABLE typed AS {TYPED_ROW}")

    assert table_rows(connection, "typed") == [
        {
            "id": "7",
            "name": "ACME INC",
            "is_active": "true",
            "price": "1.5300000000",
            "day": "2026-11-08",
            "changed_at": "2026-11-08 14:21:00.022",
            "published_at": "2026-11-08 14:21:01",
            "loaded_at": "2026-11-08 14:21:00.123456",
            "value": "not json",
            "undecodable": "�",
            "deleted_at": None,
            "note": None,
        }
    ]


def test_values_without_a_known_spark_text_form_are_refused() -> None:
    with pytest.raises(TypeError):
        spark_text("TIMESTAMP_NS", datetime(2026, 11, 8))
    with pytest.raises(TypeError):
        spark_text("DOUBLE", 1.5)


def test_paths_are_read_from_their_files_and_names_through_the_catalog() -> None:
    assert (
        relation("s3://lake/warehouse/bronze/a") == "iceberg_scan('s3://lake/warehouse/bronze/a')"
    )
    assert relation("lake.bronze.a") == "lake.bronze.a"


def test_a_table_description_comes_from_its_current_schema() -> None:
    metadata = {
        "current-schema-id": 1,
        "schemas": [
            {"schema-id": 0, "fields": [{"name": "old", "doc": "gone"}]},
            {"schema-id": 1, "fields": [{"name": "a", "doc": "A"}, {"name": "b"}]},
        ],
        "properties": {"comment": "Table"},
    }

    assert description("lake.x.t", metadata) == {
        "table": "lake.x.t",
        "comment": "Table",
        "columns": {"a": "A", "b": None},
    }


def test_a_view_description_comes_from_the_schema_of_its_current_version() -> None:
    metadata = {
        "current-version-id": 2,
        "versions": [{"version-id": 1, "schema-id": 0}, {"version-id": 2, "schema-id": 1}],
        "schemas": [
            {"schema-id": 0, "fields": [{"name": "old", "doc": "gone"}]},
            {"schema-id": 1, "fields": [{"name": "a", "doc": "A"}]},
        ],
    }

    assert description("lake.x.v", metadata) == {
        "table": "lake.x.v",
        "comment": None,
        "columns": {"a": "A"},
    }
