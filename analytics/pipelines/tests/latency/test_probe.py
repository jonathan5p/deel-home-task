import pytest

pytest.importorskip("pyspark")

from analytics.pipelines.tests.latency.latency_setup import (
    CHANGE,
    CUSTOMER,
    CUSTOMERS_TABLE,
    ORDER,
    ORDERS_TABLE,
    TOPICS,
    FakeClock,
    FakeLanding,
    FakeLookup,
    FakeSource,
)
from pipelines.bronze.entities import CUSTOMERS, Entity
from pipelines.latency.layers import LAYERS, Layer
from pipelines.latency.probe import (
    Probe,
    ProbeSetupError,
    QuerySource,
    SourceTable,
    lowest_keys_query,
    marker_statement,
    probe_targets,
    source_tables,
)
from pipelines.latency.settings import ProbeOptions
from pipelines.latency.sql import Row
from pipelines.schemas.definitions import EntityDefinition


def probe(
    clock: FakeClock,
    *,
    source: FakeSource | None = None,
    landing: FakeLanding | None = None,
    lookups: dict[Layer, FakeLookup] | None = None,
    probes: int = 1,
    timeout_seconds: float = 30,
) -> Probe:
    return Probe(
        source=source or FakeSource(),
        landing={"customers": landing or FakeLanding()},
        lookups=lookups or {layer: FakeLookup() for layer in LAYERS[1:]},
        options=ProbeOptions(
            entities=("customers",), probes=probes, poll_seconds=1, timeout_seconds=timeout_seconds
        ),
        max_wait_seconds=10,
        timing=clock.timing(random=0.25),
    )


def test_each_entity_is_probed_in_the_source_table_its_topic_names() -> None:
    tables = source_tables(["orders", "customers"], TOPICS)

    assert [table.name for table in tables] == ["orders", "customers"]
    assert tables[1].relation == '"operations"."customers"'
    assert tables[1].key_column == "customer_id"


def test_an_entity_without_a_topic_cannot_be_probed() -> None:
    with pytest.raises(ProbeSetupError, match="no topic for products"):
        source_tables(["products"], TOPICS)


def test_a_topic_must_name_the_schema_and_table() -> None:
    with pytest.raises(ProbeSetupError, match="schema"):
        SourceTable(CUSTOMERS, "customers")


def test_an_entity_without_the_marker_audit_column_cannot_be_probed() -> None:
    definition = CUSTOMERS.definition
    unaudited = Entity(EntityDefinition(definition.name, definition.key, "", definition.columns))

    with pytest.raises(ProbeSetupError, match="updated_by"):
        SourceTable(unaudited, "prefix.operations.customers")


def test_the_lowest_keys_come_from_one_query_over_every_table() -> None:
    assert lowest_keys_query([CUSTOMERS_TABLE, ORDERS_TABLE]) == (
        "SELECT 'customers' AS entity, min(\"customer_id\") AS key"
        ' FROM "operations"."customers"'
        " UNION ALL SELECT 'orders' AS entity, min(\"order_id\") AS key"
        ' FROM "operations"."orders"'
    )


def test_one_statement_marks_every_target_row_and_counts_the_rows_it_updated() -> None:
    assert marker_statement([CUSTOMER, ORDER], -42) == (
        'WITH probe_0 AS (UPDATE "operations"."customers" SET "updated_by" = -42'
        ' WHERE "customer_id" = 7 RETURNING 1),'
        ' probe_1 AS (UPDATE "operations"."orders" SET "updated_by" = -42'
        ' WHERE "order_id" = 3 RETURNING 1)'
        " SELECT 'customers' AS entity, count(*) AS updated FROM probe_0"
        " UNION ALL SELECT 'orders' AS entity, count(*) AS updated FROM probe_1"
    )


def test_the_query_source_runs_the_statements_and_reads_the_counts_by_entity() -> None:
    statements: list[str] = []

    def query(sql: str) -> list[Row]:
        statements.append(sql)
        return [("customers", 1), ("orders", None)]

    source = QuerySource(query)

    assert source.lowest_keys([CUSTOMERS_TABLE]) == {"customers": 1, "orders": None}
    assert source.mark([CUSTOMER], -1) == {"customers": 1, "orders": None}
    assert statements == [lowest_keys_query([CUSTOMERS_TABLE]), marker_statement([CUSTOMER], -1)]


def test_a_configured_key_wins_and_the_others_default_to_the_lowest_key() -> None:
    source = FakeSource(lowest={"orders": 2})

    targets = probe_targets([CUSTOMERS_TABLE, ORDERS_TABLE], {"customers": 9}, source)

    assert [(target.name, target.key) for target in targets] == [("customers", 9), ("orders", 2)]
    assert source.asked == [("orders",)]


def test_an_empty_source_table_cannot_be_probed() -> None:
    with pytest.raises(ProbeSetupError, match="no rows"):
        probe_targets([CUSTOMERS_TABLE], {}, FakeSource())


def test_a_round_waits_a_random_part_of_the_longest_trigger_then_marks_the_rows() -> None:
    clock = FakeClock()
    source = FakeSource()
    landing = FakeLanding()

    result = probe(clock, source=source, landing=landing).run_round(1, [CUSTOMER])

    assert clock.slept[0] == 2.5
    assert result.waited_seconds == 2.5
    assert landing.marks == 1
    assert source.marks == [(("customers",), -1_002_500)]
    assert result.marker == -1_002_500
    assert result.committed_at == 1_002_500


def test_a_change_seen_everywhere_in_one_poll_has_every_layer_at_that_poll() -> None:
    clock = FakeClock()

    result = probe(clock).run_round(1, [CUSTOMER])

    (observed,) = result.observations
    assert observed.change == CHANGE
    assert observed.observed_at == dict.fromkeys(LAYERS, 1_002_500)


def test_each_layer_is_seen_at_the_first_poll_that_finds_it() -> None:
    clock = FakeClock()
    lookups = {
        Layer.BRONZE: FakeLookup(after=1),
        Layer.SILVER: FakeLookup(after=2),
        Layer.GOLD: FakeLookup(after=4),
    }

    result = probe(clock, landing=FakeLanding(after=3), lookups=lookups).run_round(1, [CUSTOMER])

    (observed,) = result.observations
    start = 1_002_500
    assert observed.observed_at == {
        Layer.LANDING: start + 2000,
        Layer.BRONZE: start + 2000,
        Layer.SILVER: start + 3000,
        Layer.GOLD: start + 5000,
    }
    assert clock.slept[1:] == [1.0] * 5


def test_later_layers_are_checked_first_so_a_poll_never_sees_them_before_earlier_ones() -> None:
    order: list[Layer] = []
    lookups = {
        layer: FakeLookup(after=None, on_check=lambda layer=layer: order.append(layer))
        for layer in LAYERS[1:]
    }

    probe(FakeClock(), lookups=lookups, timeout_seconds=0.5).run_round(1, [CUSTOMER])

    assert order[:3] == [Layer.GOLD, Layer.SILVER, Layer.BRONZE]


def test_a_poll_sleeps_only_what_is_left_of_the_interval() -> None:
    clock = FakeClock(tick=0.2)

    probe(clock, landing=FakeLanding(after=2)).run_round(1, [CUSTOMER])

    assert clock.slept[1] == pytest.approx(0.8)


def test_a_round_stops_at_the_timeout_with_the_layers_it_reached() -> None:
    clock = FakeClock()
    lookups = {
        Layer.BRONZE: FakeLookup(),
        Layer.SILVER: FakeLookup(after=None),
        Layer.GOLD: FakeLookup(after=None),
    }

    result = probe(clock, lookups=lookups, timeout_seconds=5).run_round(1, [CUSTOMER])

    (observed,) = result.observations
    assert set(observed.observed_at) == {Layer.LANDING, Layer.BRONZE}
    assert len(clock.slept) == 1 + 5


def test_a_change_that_never_lands_has_no_change_and_no_layer() -> None:
    result = probe(FakeClock(), landing=FakeLanding(after=None), timeout_seconds=2).run_round(
        1, [CUSTOMER]
    )

    (observed,) = result.observations
    assert observed.change is None
    assert observed.observed_at == {}


def test_a_round_fails_when_a_target_row_was_not_marked() -> None:
    with pytest.raises(ProbeSetupError, match="customer_id = 7"):
        probe(FakeClock(), source=FakeSource(updated=0)).run_round(1, [CUSTOMER])


def test_rounds_run_in_order_with_a_new_marker_each() -> None:
    clock = FakeClock()
    seen: list[int] = []
    source = FakeSource()

    rounds = probe(clock, source=source, probes=3).run(
        [CUSTOMER], after_round=lambda finished: seen.append(finished.number)
    )

    assert [r.number for r in rounds] == [1, 2, 3] == seen
    markers = [marker for _, marker in source.marks]
    assert len(set(markers)) == 3
    assert all(marker < -1 for marker in markers)


def test_a_marker_is_never_reused_even_when_the_clock_has_not_moved() -> None:
    clock = FakeClock()
    clock.sleep = lambda seconds: None

    rounds = probe(clock, probes=2).run([CUSTOMER])

    assert rounds[1].marker == rounds[0].marker - 1
