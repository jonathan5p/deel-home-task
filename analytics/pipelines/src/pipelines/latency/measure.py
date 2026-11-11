import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.probe import Observation, ProbeRound

PERCENTILES = (50, 90, 95)


class Hop(StrEnum):
    """The steps between a source commit and gold, each from one point in time to the next."""

    COMMIT_TO_DEBEZIUM = "commit_to_debezium"
    DEBEZIUM_TO_KAFKA = "debezium_to_kafka"
    KAFKA_TO_LANDING = "kafka_to_landing"
    LANDING_TO_BRONZE = "landing_to_bronze"
    BRONZE_TO_SILVER = "bronze_to_silver"
    SILVER_TO_GOLD = "silver_to_gold"


HOPS = tuple(Hop)


def _seconds(start: int | None, end: int | None) -> float | None:
    return None if start is None or end is None else (end - start) / 1000


def latencies(observation: Observation) -> dict[Layer, float | None]:
    """Seconds from the source commit (Debezium `source.ts_ms`) until each layer had the
    change; None for a layer the probe never saw it in."""
    change = observation.change
    committed = None if change is None else change.changed_at
    return {layer: _seconds(committed, observation.observed_at.get(layer)) for layer in LAYERS}


def hops(observation: Observation) -> dict[Hop, float | None]:
    """Seconds each hop took: from the commit to Debezium reading it (`ts_ms`), to Kafka (the
    record timestamp), then to each layer as the probe first saw it."""
    change = observation.change
    points: list[int | None] = [None, None, None]
    if change is not None:
        points = [change.changed_at, change.captured_at, change.published_at]
    points += [observation.observed_at.get(layer) for layer in LAYERS]
    return {hop: _seconds(points[i], points[i + 1]) for i, hop in enumerate(HOPS)}


def last_layer(observation: Observation) -> Layer | None:
    reached = [layer for layer in LAYERS if layer in observation.observed_at]
    return reached[-1] if reached else None


def timed_out(observation: Observation) -> bool:
    return last_layer(observation) is not LAYERS[-1]


def nearest_rank(ordered: Sequence[float], percent: float) -> float:
    """The smallest value with at least `percent` % of the values at or below it."""
    rank = max(1, math.ceil(percent / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass(frozen=True, slots=True)
class Stats:
    count: int
    minimum: float
    p50: float
    p90: float
    p95: float
    maximum: float

    def as_object(self) -> dict[str, float | int]:
        return {
            "count": self.count,
            "min": round(self.minimum, 3),
            "p50": round(self.p50, 3),
            "p90": round(self.p90, 3),
            "p95": round(self.p95, 3),
            "max": round(self.maximum, 3),
        }


def stats(values: Sequence[float | None]) -> Stats | None:
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return None
    p50, p90, p95 = (nearest_rank(ordered, percent) for percent in PERCENTILES)
    return Stats(len(ordered), ordered[0], p50, p90, p95, ordered[-1])


def _by_entity(rounds: Sequence[ProbeRound]) -> dict[str, list[Observation]]:
    observations: dict[str, list[Observation]] = {}
    for probe in rounds:
        for observation in probe.observations:
            observations.setdefault(observation.entity, []).append(observation)
    return observations


def latency_stats(rounds: Sequence[ProbeRound]) -> dict[str, dict[Layer, Stats | None]]:
    return {
        entity: {layer: stats([latencies(o)[layer] for o in observations]) for layer in LAYERS}
        for entity, observations in _by_entity(rounds).items()
    }


def hop_stats(rounds: Sequence[ProbeRound]) -> dict[str, dict[Hop, Stats | None]]:
    return {
        entity: {hop: stats([hops(o)[hop] for o in observations]) for hop in HOPS}
        for entity, observations in _by_entity(rounds).items()
    }


@dataclass(frozen=True, slots=True)
class Breach:
    probe: int
    entity: str
    layer: Layer
    seconds: float
    limit: float

    def as_object(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "entity": self.entity,
            "layer": str(self.layer),
            "seconds": round(self.seconds, 3),
            "limit": self.limit,
        }


def breaches(rounds: Sequence[ProbeRound], max_latency: Mapping[Layer, float]) -> list[Breach]:
    """Every probe whose latency to a layer exceeded that layer's limit."""
    found = []
    for probe in rounds:
        for observation in probe.observations:
            measured = latencies(observation)
            for layer, limit in max_latency.items():
                seconds = measured[layer]
                if seconds is not None and seconds > limit:
                    found.append(Breach(probe.number, observation.entity, layer, seconds, limit))
    return found


@dataclass(frozen=True, slots=True)
class Timeout:
    probe: int
    entity: str
    last_layer: Layer | None

    def as_object(self) -> dict[str, object]:
        reached = None if self.last_layer is None else str(self.last_layer)
        return {"probe": self.probe, "entity": self.entity, "last_layer": reached}


def timeouts(rounds: Sequence[ProbeRound]) -> list[Timeout]:
    return [
        Timeout(probe.number, observation.entity, last_layer(observation))
        for probe in rounds
        for observation in probe.observations
        if timed_out(observation)
    ]
