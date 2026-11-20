"""Property-based tests for the processor's exact-deduplication contract."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.application.processing import DedupDecision, Deduplicator

EVENT_IDS = st.sampled_from(("a", "b", "c", "d", "e"))
UPDATED_MS = st.integers(min_value=0, max_value=10**13)


@given(events=st.lists(st.tuples(EVENT_IDS, UPDATED_MS), max_size=100))
def test_new_ids_revisions_and_duplicates_follow_the_dedup_contract(
    events: list[tuple[str, int]],
) -> None:
    state: dict[str, int] = {}
    new_ids: set[str] = set()
    revision_ids: set[str] = set()

    for event_id, updated in events:
        previous = state.get(event_id)
        decision, new_state = Deduplicator.decide(previous, updated)

        if decision is DedupDecision.NEW:
            assert previous is None
            assert event_id not in new_ids
            new_ids.add(event_id)
            assert new_state == updated
            state[event_id] = updated
        elif decision is DedupDecision.REVISION:
            assert previous is not None
            assert updated > previous
            revision_ids.add(event_id)
            assert new_state == updated
            state[event_id] = updated
        else:
            assert decision is DedupDecision.DUPLICATE
            assert previous is not None
            assert updated <= previous
            assert new_state == previous
            assert state[event_id] == previous

    assert new_ids == {event_id for event_id, _ in events}
    assert revision_ids <= new_ids


@given(updated=UPDATED_MS)
def test_decide_is_idempotent_for_the_same_version(updated: int) -> None:
    first, state = Deduplicator.decide(None, updated)
    second, next_state = Deduplicator.decide(state, updated)

    assert first is DedupDecision.NEW
    assert second is DedupDecision.DUPLICATE
    assert next_state == state
