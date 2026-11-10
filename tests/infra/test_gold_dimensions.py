from tests.infra.gold_dimensions_setup import (
    AUDIT_ONLY,
    CUSTOMERS,
    DELETED,
    LATE_VERSION,
    PRICE_CHANGE,
    PRODUCTS,
    RESNAPSHOT,
    SINGLE_VERSION,
    GoldRuns,
    at,
    by_key,
    current_counts,
    versions,
)


def test_a_price_change_closes_the_previous_version_and_opens_a_new_one(
    gold_runs: GoldRuns,
) -> None:
    assert versions(by_key(gold_runs.second.products, PRICE_CHANGE)) == [
        ["9101", "10", "1.0000000000", at(10), at(20), "f", "f", "f"],
        ["9101", "20", "2.0000000000", at(20), "", "t", "f", "f"],
    ]
    assert by_key(gold_runs.second.public_products, PRICE_CHANGE) == [
        ["9101", "1.0000000000", at(10), at(20), "f"],
        ["9101", "2.0000000000", at(20), "", "t"],
    ]


def test_each_key_has_exactly_one_current_version_unless_it_was_deleted(
    gold_runs: GoldRuns,
) -> None:
    expected = {str(key): 1 for key in (*PRODUCTS, *CUSTOMERS)} | {str(DELETED): 0}

    for state in (gold_runs.second, gold_runs.replayed, gold_runs.late):
        assert current_counts(state) == expected


def test_replaying_every_batch_from_a_fresh_checkpoint_changes_nothing(
    gold_runs: GoldRuns,
) -> None:
    assert gold_runs.replayed == gold_runs.second


def test_a_late_older_version_goes_into_history_but_does_not_become_current(
    gold_runs: GoldRuns,
) -> None:
    assert versions(by_key(gold_runs.second.products, LATE_VERSION)) == [
        ["9102", "30", "5.0000000000", at(30), at(40), "f", "f", "f"],
        ["9102", "40", "6.0000000000", at(40), at(50), "f", "f", "f"],
        ["9102", "50", "7.0000000000", at(50), "", "t", "f", "f"],
    ]


def test_an_unchanged_resnapshot_is_folded_and_adds_no_public_version(
    gold_runs: GoldRuns,
) -> None:
    assert versions(by_key(gold_runs.first.products, RESNAPSHOT)) == [
        ["9103", "60", "3.0000000000", at(60), "", "t", "f", "f"],
        ["9103", "80", "3.0000000000", at(80), "", "f", "t", "f"],
    ]
    assert by_key(gold_runs.first.public_products, RESNAPSHOT) == [
        ["9103", "3.0000000000", at(60), "", "t"],
    ]


def test_an_audit_only_update_is_folded(gold_runs: GoldRuns) -> None:
    assert versions(by_key(gold_runs.late.customers, AUDIT_ONLY)) == [
        ["9201", "10", "Ada", at(10), "", "t", "f", "f"],
        ["9201", "15", "Ada", at(15), "", "f", "t", "f"],
    ]
    assert by_key(gold_runs.late.public_customers, AUDIT_ONLY) == [
        ["9201", "Ada", at(10), "", "t"],
    ]
    assert by_key(gold_runs.late.public_customers, SINGLE_VERSION) == [
        ["9202", "Grace", at(20), "", "t"],
    ]


def test_a_late_version_between_two_equal_ones_unfolds_the_later_one(gold_runs: GoldRuns) -> None:
    assert versions(by_key(gold_runs.second.products, RESNAPSHOT)) == [
        ["9103", "60", "3.0000000000", at(60), at(70), "f", "f", "f"],
        ["9103", "70", "4.0000000000", at(70), at(80), "f", "f", "f"],
        ["9103", "80", "3.0000000000", at(80), "", "t", "f", "f"],
    ]


def test_a_delete_is_the_last_version_and_leaves_none_current(gold_runs: GoldRuns) -> None:
    assert versions(by_key(gold_runs.second.products, DELETED)) == [
        ["9104", "90", "9.0000000000", at(90), at(100), "f", "f", "f"],
        ["9104", "100", "9.0000000000", at(100), "", "f", "f", "t"],
    ]
    assert by_key(gold_runs.second.public_products, DELETED) == [
        ["9104", "9.0000000000", at(90), at(100), "f"],
    ]


def test_neither_a_replay_nor_a_late_version_revives_a_deleted_row(gold_runs: GoldRuns) -> None:
    assert versions(by_key(gold_runs.late.products, DELETED)) == [
        ["9104", "90", "9.0000000000", at(90), at(95), "f", "f", "f"],
        ["9104", "95", "8.0000000000", at(95), at(100), "f", "f", "f"],
        ["9104", "100", "9.0000000000", at(100), "", "f", "f", "t"],
    ]
    assert [row[4] for row in by_key(gold_runs.late.public_products, DELETED)] == ["f", "f"]
