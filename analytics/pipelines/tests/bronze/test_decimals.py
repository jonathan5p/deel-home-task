import base64
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.bronze.decimals import decode_decimal, is_valid_decimal
from pipelines.bronze.entities import VARIABLE_SCALE_DECIMAL
from pipelines.lake.types import LAKE_DECIMAL


def encoded(unscaled: int, size: int | None = None) -> str:
    """Base64 of the big-endian two's-complement bytes, as Debezium sends them."""
    length = size if size is not None else (unscaled.bit_length() + 8) // 8
    return base64.b64encode(unscaled.to_bytes(length, "big", signed=True)).decode()


def debezium_decimal(unscaled: int, scale: int, size: int | None = None) -> dict[str, object]:
    return {"scale": scale, "value": encoded(unscaled, size)}


def decode(spark: SparkSession, value: dict[str, object] | None) -> tuple[Decimal | None, bool]:
    frame = spark.createDataFrame([(value,)], f"price {VARIABLE_SCALE_DECIMAL.simpleString()}")
    row = frame.select(
        decode_decimal(F.col("price")).alias("decoded"),
        is_valid_decimal(F.col("price")).alias("valid"),
    ).first()
    assert row is not None
    return row.decoded, row.valid


def test_the_readme_example_decodes_to_its_price(spark: SparkSession) -> None:
    assert decode(spark, {"scale": 2, "value": "AJk="}) == (Decimal("1.53"), True)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(debezium_decimal(153, 2), Decimal("1.53"), id="positive"),
        pytest.param(debezium_decimal(-153, 2), Decimal("-1.53"), id="negative"),
        pytest.param(debezium_decimal(0, 0), Decimal(0), id="zero"),
        pytest.param(debezium_decimal(-1, 0, size=1), Decimal(-1), id="all-ones byte"),
        pytest.param(debezium_decimal(128, 0, size=2), Decimal(128), id="high bit in second byte"),
        pytest.param(
            debezium_decimal(2**63 - 1, 10, size=8),
            Decimal(2**63 - 1).scaleb(-10),
            id="largest eight-byte value",
        ),
        pytest.param(
            debezium_decimal(-(2**63), 10, size=8),
            Decimal(-(2**63)).scaleb(-10),
            id="smallest eight-byte value",
        ),
        pytest.param(debezium_decimal(15300, 12), Decimal("0.0000000153"), id="exact above scale"),
        pytest.param(debezium_decimal(0, 15), Decimal(0), id="zero above scale"),
        pytest.param(debezium_decimal(153, -3), Decimal(153000), id="negative scale"),
    ],
)
def test_variable_scale_decimals_decode_exactly(
    spark: SparkSession, value: dict[str, object], expected: Decimal
) -> None:
    decoded, valid = decode(spark, value)

    assert decoded == expected
    assert valid


def test_decoded_values_have_the_bronze_type(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(None,)], f"price {VARIABLE_SCALE_DECIMAL.simpleString()}")

    assert frame.select(decode_decimal(F.col("price"))).schema[0].dataType == LAKE_DECIMAL


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(debezium_decimal(2**63, 0, size=9), id="more than eight bytes"),
        pytest.param(debezium_decimal(1, -30), id="overflows the precision"),
        pytest.param(debezium_decimal(1534, 12), id="inexact at the bronze scale"),
        pytest.param({"scale": 2, "value": "!!not base64"}, id="not base64"),
        pytest.param({"scale": 2, "value": ""}, id="no bytes"),
        pytest.param({"scale": None, "value": "AJk="}, id="no scale"),
        pytest.param({"scale": 2, "value": None}, id="no value"),
    ],
)
def test_undecodable_decimals_are_invalid(spark: SparkSession, value: dict[str, object]) -> None:
    assert decode(spark, value) == (None, False)


def test_a_null_decimal_is_valid(spark: SparkSession) -> None:
    assert decode(spark, None) == (None, True)
