from tests.infra.gold_facts_setup import (
    AFTER_PRICE_CHANGE,
    BEFORE_ITS_ORDER,
    BEFORE_ITS_PRODUCT,
    BEFORE_PRICE_CHANGE,
    CHANGE_SEQUENCE,
    CUSTOMER_LATER,
    DELETED,
    EARLY_ITEM_PRODUCT,
    IN_SPLIT_RANGE,
    IN_UNFOLDED_VERSION,
    ITEMS_FIRST,
    OUT_OF_ORDER,
    REOPENED,
    RESNAPSHOT,
    UNFOLDED_ORDER,
    FactRuns,
    by_key,
)
from tests.infra.gold_silver_setup import at

PENDING, PROCESSING, COMPLETED, UNKNOWN = "1", "2", "4", "5"
ORDER_DAY, DELIVERY_DAY = "20261101", "20261105"


def test_order_history_follows_the_source_change_sequence(fact_runs: FactRuns) -> None:
    assert by_key(fact_runs.second.public_order_history, CHANGE_SEQUENCE) == [
        ["9501", at(20), "create", PENDING, "5", ORDER_DAY, DELIVERY_DAY, at(40), "f"],
        ["9501", at(40), "update", PROCESSING, "5", ORDER_DAY, DELIVERY_DAY, at(60), "f"],
        ["9501", at(60), "update", COMPLETED, "5", ORDER_DAY, DELIVERY_DAY, "", "t"],
    ]


def test_the_current_state_holds_the_latest_version_of_each_order(fact_runs: FactRuns) -> None:
    current = fact_runs.second.current_orders

    assert by_key(current, CHANGE_SEQUENCE) == [
        ["9501", COMPLETED, "9401", ORDER_DAY, DELIVERY_DAY, "60", at(60), "f"]
    ]
    assert [row[5] for row in current] == ["60", "100", "120", "140", "395", "300", "550", "720"]


def test_replaying_every_change_from_a_fresh_checkpoint_changes_nothing(
    fact_runs: FactRuns,
) -> None:
    assert fact_runs.replayed == fact_runs.second


def test_an_older_change_arriving_late_goes_into_history_but_not_into_current_state(
    fact_runs: FactRuns,
) -> None:
    assert by_key(fact_runs.first.current_orders, OUT_OF_ORDER)[0][1] == PROCESSING
    assert by_key(fact_runs.second.public_order_history, OUT_OF_ORDER) == [
        ["9502", at(90), "create", PENDING, "5", ORDER_DAY, DELIVERY_DAY, at(100), "f"],
        ["9502", at(100), "update", PROCESSING, "5", ORDER_DAY, DELIVERY_DAY, "", "t"],
    ]
    assert by_key(fact_runs.second.current_orders, OUT_OF_ORDER)[0][1::4] == [PROCESSING, "100"]


def test_a_delete_hides_the_order_and_an_older_change_never_revives_it(
    fact_runs: FactRuns,
) -> None:
    for state in (fact_runs.first, fact_runs.replayed, fact_runs.late):
        assert by_key(state.current_orders, DELETED)[0][5:] == ["120", at(120), "t"]
        assert by_key(state.public_current_orders, DELETED) == []
    assert by_key(fact_runs.late.public_order_history, DELETED) == [
        ["9503", at(110), "create", UNKNOWN, "5", ORDER_DAY, DELIVERY_DAY, at(115), "f"],
        ["9503", at(115), "update", PROCESSING, "5", ORDER_DAY, DELIVERY_DAY, at(120), "f"],
        ["9503", at(120), "delete", UNKNOWN, "5", ORDER_DAY, DELIVERY_DAY, "", "f"],
    ]


def test_an_unchanged_resnapshot_is_folded_out_of_the_public_history(
    fact_runs: FactRuns,
) -> None:
    assert [row[1] for row in by_key(fact_runs.first.order_history, RESNAPSHOT)] == ["130", "140"]
    assert by_key(fact_runs.first.public_order_history, RESNAPSHOT) == [
        ["9504", at(130), "create", UNKNOWN, "5", ORDER_DAY, "", "", "t"],
    ]
    assert by_key(fact_runs.first.current_orders, RESNAPSHOT)[0][4:6] == ["", "140"]


def test_an_item_takes_the_price_of_the_product_version_valid_when_it_changed(
    fact_runs: FactRuns,
) -> None:
    history = fact_runs.second.item_history

    assert by_key(history, BEFORE_PRICE_CHANGE) == [
        ["9601", "21", "10", "1.0000000000", "3.0000000000"]
    ]
    assert by_key(history, AFTER_PRICE_CHANGE) == [
        ["9602", "35", "30", "2.0000000000", "4.0000000000"]
    ]


def test_an_item_arriving_before_its_order_counts_once_the_order_arrives(
    fact_runs: FactRuns,
) -> None:
    assert by_key(fact_runs.first.current_items, BEFORE_ITS_ORDER) == [
        ["9603", str(ITEMS_FIRST), str(EARLY_ITEM_PRODUCT), "4", "400", "f"]
    ]
    assert by_key(fact_runs.first.pending_items, EARLY_ITEM_PRODUCT) == []
    assert by_key(fact_runs.second.pending_items, EARLY_ITEM_PRODUCT) == [["9303", "1", "4"]]


def test_an_order_arriving_before_its_customer_sees_it_once_the_customer_arrives(
    fact_runs: FactRuns,
) -> None:
    assert by_key(fact_runs.first.order_history, CUSTOMER_LATER)[0][5] == ""
    assert by_key(fact_runs.second.order_history, CUSTOMER_LATER)[0][5] == "290"


def test_a_late_dimension_version_moves_the_facts_it_covers_to_that_version_when_read(
    fact_runs: FactRuns,
) -> None:
    first, second = fact_runs.first, fact_runs.second

    assert by_key(first.item_history, IN_UNFOLDED_VERSION) == [
        ["9604", "250", "200", "3.0000000000", "6.0000000000"]
    ]
    assert by_key(second.item_history, IN_UNFOLDED_VERSION) == [
        ["9604", "250", "240", "3.0000000000", "6.0000000000"]
    ]
    assert by_key(first.item_history, IN_SPLIT_RANGE) == [
        ["9605", "230", "200", "3.0000000000", "3.0000000000"]
    ]
    assert by_key(second.item_history, IN_SPLIT_RANGE) == [
        ["9605", "230", "220", "4.0000000000", "4.0000000000"]
    ]
    assert by_key(first.order_history, UNFOLDED_ORDER)[0][5] == "500"
    assert by_key(second.order_history, UNFOLDED_ORDER)[0][5] == "540"


def test_an_item_snapshotted_just_before_its_product_takes_the_first_product_version(
    fact_runs: FactRuns,
) -> None:
    for state in (fact_runs.first, fact_runs.second):
        assert by_key(state.item_history, BEFORE_ITS_PRODUCT) == [
            ["9606", "600", "600", "7.0000000000", "14.0000000000"]
        ]


def test_an_order_is_open_again_after_each_status_change_and_not_after_a_late_one(
    fact_runs: FactRuns,
) -> None:
    flags = [
        by_key(state.open_flags, REOPENED)
        for state in (fact_runs.first, fact_runs.second, fact_runs.replayed, fact_runs.late)
    ]

    assert flags == [[["9508", "t"]], [["9508", "f"]], [["9508", "f"]], [["9508", "t"]]]
    assert by_key(fact_runs.second.open_orders, REOPENED) == []
    assert by_key(fact_runs.late.open_orders, REOPENED) == [["9508"]]
    assert by_key(fact_runs.late.open_flags, CHANGE_SEQUENCE) == [["9501", "f"]]


def test_a_deleted_order_is_never_an_open_order(fact_runs: FactRuns) -> None:
    for state in (fact_runs.first, fact_runs.replayed, fact_runs.late):
        assert by_key(state.open_flags, DELETED) == [["9503", "t"]]
        assert by_key(state.open_orders, DELETED) == []
