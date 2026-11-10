from tests.infra.bronze_replay_setup import ReplayRuns, commit_sequences
from tests.infra.bronze_setup import FIRST_LSN


def test_replaying_a_range_already_in_bronze_adds_nothing(replay_runs: ReplayRuns) -> None:
    bronze, quarantine = replay_runs.streamed

    assert (bronze.offsets, quarantine.offsets) == ([0, 1], [2])
    assert replay_runs.replayed_ingested == replay_runs.streamed
    assert "0 changes appended, 2 already in bronze" in replay_runs.summaries[0]
    assert "0 quarantined, 1 already quarantined" in replay_runs.summaries[0]


def test_replaying_a_range_with_missing_changes_adds_each_once(replay_runs: ReplayRuns) -> None:
    bronze, _ = replay_runs.replayed_missing

    assert commit_sequences(bronze) == [FIRST_LSN + lsn for lsn in (0, 1, 3, 4)]
    assert "2 changes appended, 2 already in bronze" in replay_runs.summaries[1]


def test_a_failure_replayed_twice_is_quarantined_once(replay_runs: ReplayRuns) -> None:
    _, quarantine = replay_runs.replayed_again

    assert quarantine.offsets == [2, 5]
    assert "1 quarantined, 1 already quarantined" in replay_runs.summaries[1]
    assert "0 quarantined, 2 already quarantined" in replay_runs.summaries[2]


def test_a_second_replay_of_the_same_range_appends_nothing(replay_runs: ReplayRuns) -> None:
    assert replay_runs.replayed_again == replay_runs.replayed_missing
    assert "0 changes appended, 4 already in bronze" in replay_runs.summaries[2]
