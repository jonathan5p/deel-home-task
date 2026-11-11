import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.latency_setup import (
    ALL_LAYERS_SECONDS,
    observation,
    probe_round,
)
from pipelines.latency.layers import Layer
from pipelines.latency.measure import (
    Breach,
    Hop,
    Stats,
    Timeout,
    breaches,
    hop_stats,
    hops,
    last_layer,
    latencies,
    latency_stats,
    nearest_rank,
    stats,
    timed_out,
    timeouts,
)

PARTIAL = {Layer.LANDING: 2.5, Layer.BRONZE: 12}


def test_the_latency_of_each_layer_runs_from_the_source_commit() -> None:
    assert latencies(observation(seconds=ALL_LAYERS_SECONDS)) == {
        Layer.LANDING: 3.0,
        Layer.BRONZE: 13.0,
        Layer.SILVER: 23.0,
        Layer.GOLD: 33.0,
    }


def test_a_layer_never_reached_has_no_latency() -> None:
    measured = latencies(observation(seconds=PARTIAL))

    assert measured[Layer.BRONZE] == 12.0
    assert measured[Layer.SILVER] is None
    assert measured[Layer.GOLD] is None


def test_a_change_that_never_landed_has_no_latencies() -> None:
    assert set(latencies(observation(change=None)).values()) == {None}


def test_the_hops_split_the_latency_from_commit_to_gold() -> None:
    measured = hops(observation(seconds=ALL_LAYERS_SECONDS))

    assert measured == {
        Hop.COMMIT_TO_DEBEZIUM: 0.1,
        Hop.DEBEZIUM_TO_KAFKA: 0.2,
        Hop.KAFKA_TO_LANDING: 2.7,
        Hop.LANDING_TO_BRONZE: 10.0,
        Hop.BRONZE_TO_SILVER: 10.0,
        Hop.SILVER_TO_GOLD: 10.0,
    }
    assert sum(value or 0 for value in measured.values()) == pytest.approx(33.0)


def test_hops_after_the_last_layer_reached_are_unknown() -> None:
    measured = hops(observation(seconds=PARTIAL))

    assert measured[Hop.LANDING_TO_BRONZE] == 9.5
    assert measured[Hop.BRONZE_TO_SILVER] is None
    assert hops(observation(change=None))[Hop.COMMIT_TO_DEBEZIUM] is None


def test_the_last_layer_reached_tells_a_timeout_from_a_full_run() -> None:
    assert last_layer(observation(seconds=PARTIAL)) is Layer.BRONZE
    assert last_layer(observation(change=None)) is None
    assert timed_out(observation(seconds=PARTIAL))
    assert not timed_out(observation(seconds=ALL_LAYERS_SECONDS))


@pytest.mark.parametrize(
    ("percent", "expected"), [(50, 5.0), (90, 9.0), (95, 10.0), (100, 10.0), (0, 1.0)]
)
def test_nearest_rank_picks_the_smallest_value_with_that_share_at_or_below_it(
    percent: float, expected: float
) -> None:
    assert nearest_rank([float(value) for value in range(1, 11)], percent) == expected


def test_stats_ignore_missing_values_and_sort_the_rest() -> None:
    assert stats([3.0, None, 1.0, 2.0]) == Stats(3, 1.0, 2.0, 3.0, 3.0, 3.0)
    assert stats([None]) is None


def test_stats_render_rounded_to_milliseconds() -> None:
    assert Stats(2, 1.00049, 1.5, 2.0, 2.0, 2.0).as_object() == {
        "count": 2,
        "min": 1.0,
        "p50": 1.5,
        "p90": 2.0,
        "p95": 2.0,
        "max": 2.0,
    }


def test_latency_and_hop_stats_are_per_entity_over_every_round() -> None:
    rounds = [
        probe_round(1, observation(seconds=ALL_LAYERS_SECONDS), observation("orders")),
        probe_round(2, observation(seconds=PARTIAL)),
    ]

    by_layer = latency_stats(rounds)
    by_hop = hop_stats(rounds)

    assert by_layer["customers"][Layer.LANDING] == Stats(2, 2.5, 2.5, 3.0, 3.0, 3.0)
    assert by_layer["customers"][Layer.GOLD] == Stats(1, 33.0, 33.0, 33.0, 33.0, 33.0)
    assert by_layer["orders"][Layer.LANDING] is None
    assert by_hop["customers"][Hop.SILVER_TO_GOLD] == Stats(1, 10.0, 10.0, 10.0, 10.0, 10.0)


def test_a_breach_is_a_probe_slower_than_its_layers_limit() -> None:
    rounds = [
        probe_round(1, observation(seconds=ALL_LAYERS_SECONDS)),
        probe_round(2, observation(seconds=PARTIAL)),
    ]

    found = breaches(rounds, {Layer.GOLD: 30, Layer.BRONZE: 12.5, Layer.LANDING: 3})

    assert found == [
        Breach(1, "customers", Layer.GOLD, 33.0, 30),
        Breach(1, "customers", Layer.BRONZE, 13.0, 12.5),
    ]
    assert found[0].as_object() == {
        "probe": 1,
        "entity": "customers",
        "layer": "gold",
        "seconds": 33.0,
        "limit": 30,
    }


def test_a_timeout_is_a_probe_that_never_reached_gold() -> None:
    rounds = [
        probe_round(1, observation(seconds=ALL_LAYERS_SECONDS), observation("orders", change=None)),
        probe_round(2, observation(seconds=PARTIAL)),
    ]

    found = timeouts(rounds)

    assert found == [Timeout(1, "orders", None), Timeout(2, "customers", Layer.BRONZE)]
    assert [t.as_object()["last_layer"] for t in found] == [None, "bronze"]
