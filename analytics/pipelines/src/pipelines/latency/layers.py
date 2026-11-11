from enum import StrEnum


class Layer(StrEnum):
    """The places a source change becomes visible in, in the order it reaches them."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


LAYERS = tuple(Layer)
