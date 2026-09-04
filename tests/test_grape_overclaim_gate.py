"""MEASUREMENT_MODEL §9 invariant 5: the published uncertainty never falls
below the observed scatter.  A claim better than reality fails a gate,
not a paper two years later (TIMING_PROVENANCE_MODEL §8)."""
from hamsci_physics.grape.overclaim_gate import assess_day


def _state(t, origin, u_ns, k=2, space="AC0G-B4-status.local/T6_96000"):
    return {"t": t, "type": "state", "schema": "v2", "counter_space": space, "counter_epoch_id": "r-1",
            "origin": origin, "u_epoch_ns": u_ns, "k": k}


def _row(t, t5=None, t2=None):
    return {"utc_published": t, "t5_available": 1 if t5 is not None else 0,
            "t5_offset_ms": t5, "host_clock_t2_ms": t2, "host_clock_verdict": "fault" if t2 else "ok"}


def test_native_anchor_claim_below_t5_scatter_is_an_overclaim():
    states = [_state("2026-09-04T16:00:00Z", "native_anchor", 4_093)]      # 4.1 us, k=2 -> 8.2 us claim
    rows = [_row(f"2026-09-04T16:0{i}:00Z", t5=(-1.0) ** i * 1.2) for i in range(10)]   # 1.2 ms scatter
    rep = assess_day(states, rows)
    (v,) = rep.intervals
    assert v.witness == "T5" and v.overclaim is True and rep.overclaims == 1
    assert abs(v.observed_ms - 1.2) < 1e-9


def test_native_anchor_claim_wider_than_scatter_passes():
    states = [_state("2026-09-04T16:00:00Z", "native_anchor", 2_000_000)]  # 2 ms, k=2 -> 4 ms
    rows = [_row(f"2026-09-04T16:0{i}:00Z", t5=(-1.0) ** i * 1.2) for i in range(10)]
    assert assess_day(states, rows).overclaims == 0


def test_sysclock_claim_below_the_witnessed_disagreement_is_an_overclaim():
    # The record B4 would have written at 15:00Z with the p99 stand-in
    # alone, against a T2 witness reading 11.6 s.
    states = [_state("2026-09-04T15:00:00Z", "sysclock", 8_030_000, k=1)]
    rows = [_row(f"2026-09-04T15:0{i}:00Z", t2=11_640.0) for i in range(10)]
    (v,) = assess_day(states, rows).intervals
    assert v.witness == "host_clock_t2_ms" and v.overclaim is True and v.observed_ms == 11_640.0


def test_sysclock_claim_bounded_by_the_host_clock_passes():
    states = [_state("2026-09-04T15:00:00Z", "sysclock", 11_640_000_000, k=1)]
    rows = [_row(f"2026-09-04T15:0{i}:00Z", t2=11_640.0) for i in range(10)]
    assert assess_day(states, rows).overclaims == 0


def test_no_witness_rows_is_unwitnessed_not_failure():
    states = [_state("2026-09-04T16:00:00Z", "native_anchor", 4_093)]
    rep = assess_day(states, [])
    assert rep.overclaims == 0 and rep.unwitnessed == 1


def test_absence_is_never_an_overclaim():
    states = [_state("2026-09-04T16:00:00Z", None, None)]
    rep = assess_day(states, [_row("2026-09-04T16:01:00Z", t5=5.0)])
    assert rep.overclaims == 0 and rep.unwitnessed == 1


def test_report_round_trips_to_dict():
    rep = assess_day([_state("2026-09-04T16:00:00Z", "native_anchor", 4_093)], [])
    d = rep.to_dict()
    assert d["date"] == "2026-09-04" and d["intervals"][0]["reason"]
