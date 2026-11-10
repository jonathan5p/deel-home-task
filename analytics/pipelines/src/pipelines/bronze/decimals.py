from pyspark.sql import Column
from pyspark.sql import functions as F

from pipelines.lake.types import LAKE_DECIMAL

MAX_UNSCALED_BYTES = 8


def _signed_unscaled(raw: Column) -> Column:
    """The big-endian two's-complement bytes as a signed decimal string.

    The hex digits are sign-extended to 64 bits so that `conv` with a negative target base
    reads them as a signed long.
    """
    digits = F.hex(raw)
    width = 2 * MAX_UNSCALED_BYTES
    negative = F.substring(digits, 1, 1) >= F.lit("8")
    extended = F.when(negative, F.lpad(digits, width, "F")).otherwise(F.lpad(digits, width, "0"))
    return F.conv(extended, 16, -10)


def decode_decimal(value: Column) -> Column:
    """Decode Debezium's variable-scale decimal `{"scale": n, "value": "<base64>"}`.

    The source column has no declared precision, so Debezium sends the unscaled value as
    big-endian bytes plus a per-value scale. Returns a DECIMAL(38,10), or null when the value is
    null, undecodable, wider than eight bytes or not exactly representable at the lake scale:
    `try_cast` to a decimal rounds silently, so a value that does not fit exactly is left for
    quarantine instead of being rounded.
    """
    raw = F.try_to_binary(value["value"], F.lit("base64"))
    scale = value["scale"]
    unscaled = _signed_unscaled(raw)
    fits_the_scale = (
        (scale <= LAKE_DECIMAL.scale)
        | (unscaled == F.lit("0"))
        | F.endswith(unscaled, F.repeat(F.lit("0"), scale - LAKE_DECIMAL.scale))
    )
    exact = F.concat(unscaled, F.lit("E"), (-scale).cast("string")).try_cast(LAKE_DECIMAL)
    decodable = F.octet_length(raw).between(1, MAX_UNSCALED_BYTES) & fits_the_scale
    return F.when(decodable, exact)


def is_valid_decimal(value: Column) -> Column:
    """True for a null value or one that decodes; false for anything to quarantine."""
    return value.isNull() | decode_decimal(value).isNotNull()
