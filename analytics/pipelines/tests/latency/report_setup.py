from collections.abc import Mapping

from analytics.pipelines.tests.latency.latency_setup import (
    ALL_LAYERS_SECONDS,
    CUSTOMER,
    ORDER,
    observation,
    probe_round,
)
from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.probe import ProbeRound
from pipelines.latency.report import RunReport
from pipelines.latency.settings import ProbeOptions

STARTED_AT = 1_794_426_000_000
TRIGGERS = dict.fromkeys(LAYERS, 10)
PASSING_ROUNDS = (
    probe_round(
        1,
        observation(seconds=ALL_LAYERS_SECONDS),
        observation("orders", seconds=ALL_LAYERS_SECONDS),
    ),
    probe_round(
        2,
        observation(seconds=ALL_LAYERS_SECONDS),
        observation("orders", seconds=ALL_LAYERS_SECONDS),
    ),
)
TIMED_OUT_ROUNDS = (
    probe_round(
        1,
        observation("customers", seconds={Layer.LANDING: 3, Layer.BRONZE: 13}),
        observation("orders", change=None),
    ),
)


def run_report(
    rounds: tuple[ProbeRound, ...] = PASSING_ROUNDS,
    *,
    max_latency: Mapping[Layer, float] | None = None,
) -> RunReport:
    return RunReport(
        started_at=STARTED_AT,
        finished_at=STARTED_AT + 90_000,
        options=ProbeOptions(
            entities=("customers", "orders"), probes=len(rounds), max_latency=max_latency or {}
        ),
        trigger_seconds=TRIGGERS,
        targets=(CUSTOMER, ORDER),
        rounds=rounds,
    )
