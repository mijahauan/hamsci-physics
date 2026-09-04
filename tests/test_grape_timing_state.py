"""The consumer reads u_epoch_ns and stability_ns, never judge_tier
(MEASUREMENT_MODEL §8).  A legacy block decodes exactly as before."""
import json
from pathlib import Path

import pytest

from hamsci_physics.grape.timing_state import (
    UNKNOWN, TimingState, grade_for, timing_state_from_sidecar,
)

GOLDEN = Path(__file__).parent / "golden" / "timing_state_b4_20260904T1600Z.json"


def _v2(**over):
    rec = json.loads(GOLDEN.read_text())
    rec.update(over)
    return {"channel_name": "T6_96000", "timing": rec}


def test_v2_block_reads_u_epoch_and_names_epoch_and_origin():
    s = timing_state_from_sidecar(_v2())
    assert s.schema == "v2" and s.origin == "native_anchor"
    assert s.uncertainty_ms == pytest.approx(0.004093)
    assert s.quality_grade == "A"
    assert s.counter_epoch_id == "r-2026-09-04T15:53:35Z"


def test_v2_d_clock_comes_from_the_legacy_mirror_or_engineering_offset():
    # Plan B mirrors offset_ns at top level for one release; afterwards it
    # lives only under engineering.  Both spellings decode the same.
    s1 = timing_state_from_sidecar(_v2(offset_ns=3_536_564.66))
    rec = json.loads(GOLDEN.read_text()); rec["engineering"]["offset_ns"] = 3_536_564.66
    s2 = timing_state_from_sidecar({"timing": rec})
    assert s1.d_clock_ms == pytest.approx(3.53656466) == s2.d_clock_ms


def test_v2_block_never_reads_judge_tier():
    rec = json.loads(GOLDEN.read_text())
    rec["engineering"]["judge_tier"] = "T0"      # a lie the tier could tell
    s = timing_state_from_sidecar({"timing": rec})
    assert s.quality_grade == "A"                # u_epoch_ns decides, not the tier


def test_v2_absence_is_the_unknown_sentinel():
    s = timing_state_from_sidecar(_v2(origin=None, u_epoch_ns=None, reason="lock_not_credible"))
    assert (s.d_clock_ms, s.uncertainty_ms, s.quality_grade) == (0.0, 999.9, "X")
    assert s.counter_epoch_id == "r-2026-09-04T15:53:35Z"   # the epoch is still known


def test_sysclock_record_bounded_by_the_host_clock_grades_d():
    s = timing_state_from_sidecar(_v2(origin="sysclock", chain="sysclock@1",
                                       u_epoch_ns=11_640_000_000, k=1, p=1.0))
    assert s.uncertainty_ms == 11_640.0 and s.quality_grade == "D"


def test_legacy_block_decodes_exactly_as_before():
    legacy = {"channel_name": "SHARED_5000",
              "timing": {"offset_ns": 3_536_564.66, "offset_sigma_ns": 982_495.95, "judge_tier": "T6"}}
    s = timing_state_from_sidecar(legacy)
    assert s.schema is None and s.origin is None and s.counter_epoch_id is None
    assert (s.d_clock_ms, s.uncertainty_ms, s.quality_grade) == (
        pytest.approx(3.53656466), pytest.approx(0.98249595), "A")


def test_no_block_and_no_meta_are_unknown():
    assert timing_state_from_sidecar({"channel_name": "x"}) == UNKNOWN
    assert timing_state_from_sidecar(None) == UNKNOWN


@pytest.mark.parametrize("ms,letter", [(0.98, "A"), (2.0, "B"), (4.0, "C"), (8.0, "D"), (999.9, "D")])
def test_grade_ladder_unchanged(ms, letter):
    assert grade_for(ms) == letter


def test_decimation_pipeline_delegates():
    from hamsci_physics.grape.decimation_pipeline import timing_from_sidecar
    assert timing_from_sidecar(_v2()) == (pytest.approx(0.0), pytest.approx(0.004093), "A")
