# GRAPE timing state, chain sidecar and overclaim gate Implementation Plan (hamsci-physics)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The GRAPE consumer reads the v2 `state` record — `u_epoch_ns` and `stability_ns`, never `judge_tier` — refuses to extrapolate across a `counter_epoch_id` change, writes the day's chain sidecar beside the OBS output, and runs the overclaim gate that compares every published uncertainty against the scatter an independent registration observed over the same interval.

**Architecture:** `grape/timing_state.py` (new, pure) parses a chunk's `timing` block into a `TimingState` and keeps the legacy grade ladder for products; `decimation_pipeline.timing_from_sidecar` delegates to it and `DecimatedBuffer` records `counter_epoch_id` and `origin` per minute; `grape/timing_chain_sidecar.py` (new) writes `timing-chain-YYYYMMDD.jsonl` from `/run/hf-timestd/timing_chain.json` and the day's chunk state records under mag-recorder's change-plus-heartbeat policy, into `<data_root>/timing/`, outside the PSWS payload; `grape/overclaim_gate.py` (new, pure) evaluates the day's state records against the authority history store's T5 residuals and host-clock columns; the CLI gains `grape timing-chain` and `grape timing-gate`, and the daily pipeline runs both best-effort.

**Tech Stack:** Python ≥3.10, hamsci-dsp ≥ 0.5.0 with `hamsci_dsp.timing_map` (Plan A) and the four `host_clock_*` columns (Plan A Task 5), sqlite3 (history store), pytest via `.venv/bin/python -m pytest tests/<file> -p no:cacheprovider --override-ini="addopts=" -q`.

**Spec:** `/home/mjh/hamsci/repos/hf-timestd/docs/design/TIMING_PROVENANCE_MODEL.md` (amended 2026-09-04) §3.0, §3.0.1, §3.1, §3.4, §6 deliverables 3–5, §8; `/home/mjh/hamsci/repos/hf-timestd/docs/design/MEASUREMENT_MODEL.md` §3, §5, §8, §9 invariant 5.

## Global Constraints

- Depends on hamsci-dsp Plan A (schema) and hf-timestd Plan B (producer writes the v2 block). Legacy chunks (no `schema` in `timing`) must keep decoding exactly as today.
- Consumers read `u_epoch_ns` and `stability_ns`, never `judge_tier` (MEASUREMENT_MODEL §8). The tier may be logged, never branched on.
- No consumer extrapolates across a change in `counter_epoch_id` (MEASUREMENT_MODEL §3).
- Absence stays absence: a v2 block with `origin: null` or `u_epoch_ns: null` yields the unknown sentinels `(0.0, 999.9, "X")` (spec §7).
- The A/B/C/D grade ladder (2/4/8 ms) stays for the fusion chain and is noted as having no resolution for a µs-class chain (spec §3.0). Not replaced here.
- **Spec deviation, recorded:** §3.0.1 says the chain sidecar is "bundled into the OBS zip". PSWS ingestion keys on payload convention, and mag-recorder learned on 2026-08-21 that an extra file in the payload is stored but never ingested (`mag_recorder/core/packager.py` docstring). GRAPE's PSWS server also rejects unexpected DRF metadata ("UNPLANNED FREQUENCY IN METADATA"). So the sidecar lands at `<data_root>/timing/timing-chain-YYYYMMDD.jsonl`, outside the OBS directory, and is not uploaded. Including it in the PSWS payload waits on a PSWS acceptance test; the plan leaves a config key `include_in_upload = false` and an explicit TODO-free note in the docs naming that test as the licensing condition.
- Sidecar writing is best-effort: never raises into the daily pipeline (spec §7).
- The overclaim gate is report-only in the daily pipeline (exit code logged, never fatal) and exits 1 from its own CLI when any interval overclaims (spec §8).
- Commit on `main`; trailer on every commit.

---

## File map

| file | responsibility |
|---|---|
| `src/hamsci_physics/grape/timing_state.py` (create) | `TimingState`, `timing_state_from_sidecar(meta)`, grade ladder |
| `src/hamsci_physics/grape/decimation_pipeline.py` (modify) | `timing_from_sidecar` delegates; passes `counter_epoch_id`/`origin` to `write_minute` |
| `src/hamsci_physics/grape/decimated_buffer.py` (modify) | `MinuteMetadata.counter_epoch_id`, `.origin`; `DayMetadata` summary `counter_epochs`, `origin_switches` |
| `src/hamsci_physics/grape/timing_chain_sidecar.py` (create) | day sidecar writer |
| `src/hamsci_physics/grape/overclaim_gate.py` (create) | `assess_day(...)`, `GateReport` |
| `src/hamsci_physics/cli.py` (modify) | `grape timing-chain`, `grape timing-gate` |
| `src/hamsci_physics/grape/daily_pipeline.py` (modify; locate with `grep -rn "def run_daily\|package_day(" src/hamsci_physics/grape/`) | call both, best-effort |
| tests: `tests/test_grape_timing_state.py`, `tests/test_grape_counter_epoch.py`, `tests/test_grape_timing_chain_sidecar.py`, `tests/test_grape_overclaim_gate.py`, `tests/test_cli_timing_commands.py` (create); `tests/test_grape_timing_metadata.py` (keep passing) | |
| `docs/TIMING_STATE.md` (create) | what the consumer reads and why; the upload licensing condition |

---

### Task 1: `TimingState` — read the v2 block, keep the legacy path byte-for-byte

**Files:**
- Create: `src/hamsci_physics/grape/timing_state.py`
- Modify: `src/hamsci_physics/grape/decimation_pipeline.py:26-56` (`timing_from_sidecar` becomes a thin delegate; `_GRADE_LADDER`, `UNKNOWN_TIMING` move to the new module and are re-exported)
- Test: `tests/test_grape_timing_state.py` (create); `tests/test_grape_timing_metadata.py` (must still pass unchanged)

**Interfaces:**
- Produces: `TimingState(d_clock_ms: float, uncertainty_ms: float, quality_grade: str, counter_epoch_id: Optional[str], origin: Optional[str], schema: Optional[str])`; `timing_state_from_sidecar(meta: Optional[dict]) -> TimingState`; `UNKNOWN = TimingState(0.0, 999.9, "X", None, None, None)`; `grade_for(uncertainty_ms: float) -> str`; `GRADE_LADDER = ((2.0, "A"), (4.0, "B"), (8.0, "C"))`.
- `decimation_pipeline.timing_from_sidecar(meta) -> (d_clock_ms, uncertainty_ms, grade)` keeps its signature and returns `TimingState`'s first three fields.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_grape_timing_state.py
"""The consumer reads u_epoch_ns and stability_ns, never judge_tier
(MEASUREMENT_MODEL §8).  A legacy block decodes exactly as before."""
import json
from pathlib import Path

import pytest

from hamsci_physics.grape.timing_state import (
    UNKNOWN, TimingState, grade_for, timing_state_from_sidecar,
)

GOLDEN = Path(__file__).parent / "golden" / "timing_state_b4_20260904T1610Z.json"


def _v2(**over):
    rec = json.loads(GOLDEN.read_text())
    rec.update(over)
    return {"channel_name": "T6_96000", "timing": rec}


def test_v2_block_reads_u_epoch_and_names_epoch_and_origin():
    s = timing_state_from_sidecar(_v2())
    assert s.schema == "v2" and s.origin == "native_anchor"
    assert s.uncertainty_ms == pytest.approx(0.004093)
    assert s.quality_grade == "A"
    assert s.counter_epoch_id == "r-2026-09-04T16:03:35Z"


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
    assert s.counter_epoch_id == "r-2026-09-04T16:03:35Z"   # the epoch is still known


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
```

Copy the golden file from hamsci-dsp: `mkdir -p tests/golden && cp /home/mjh/hamsci/repos/hamsci-dsp/tests/golden/timing_state_b4_20260904T1610Z.json tests/golden/`.

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_timing_state.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `ModuleNotFoundError: No module named 'hamsci_physics.grape.timing_state'`

- [ ] **Step 3: Implement**

```python
# src/hamsci_physics/grape/timing_state.py
"""What the GRAPE consumer reads from a chunk's `timing` block.

TIMING_PROVENANCE_MODEL §3.1: the block is the schema v2 `state` record —
the registration in force and its uncertainty.  MEASUREMENT_MODEL §8:
consumers read u_epoch_ns and stability_ns, never a tier.  A legacy block
(no `schema`) carries the Offset Judge verdict and decodes as it always did.

The A/B/C/D ladder (2 / 4 / 8 ms) stays for the fusion chain.  It has no
resolution for a µs-class chain — every payload-anchored chunk grades "A" —
and choosing its replacement belongs with Phase 3 (spec §3.0).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

GRADE_LADDER = ((2.0, "A"), (4.0, "B"), (8.0, "C"))


@dataclass(frozen=True)
class TimingState:
    d_clock_ms: float
    uncertainty_ms: float
    quality_grade: str          # A, B, C, D, or X for "no verdict"
    counter_epoch_id: Optional[str]
    origin: Optional[str]
    schema: Optional[str]

    @property
    def legacy_tuple(self):
        return (self.d_clock_ms, self.uncertainty_ms, self.quality_grade)


UNKNOWN = TimingState(0.0, 999.9, "X", None, None, None)


def grade_for(uncertainty_ms: float) -> str:
    for bound, letter in GRADE_LADDER:
        if uncertainty_ms < bound:
            return letter
    return "D"


def _opt_float(v) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def timing_state_from_sidecar(meta: Optional[dict]) -> TimingState:
    if not meta:
        return UNKNOWN
    # Flat keys still win where a producer supplies them (unchanged).
    if meta.get("uncertainty_ms") is not None:
        return TimingState(float(meta.get("d_clock_ms", 0.0)), float(meta["uncertainty_ms"]),
                           str(meta.get("quality_grade", "X")), None, None, None)
    timing = meta.get("timing") or {}
    schema = timing.get("schema")
    if schema == "v2":
        eng = timing.get("engineering") or {}
        epoch = timing.get("counter_epoch_id")
        origin = timing.get("origin")
        u_ns = _opt_float(timing.get("u_epoch_ns"))
        if origin is None or u_ns is None:
            return TimingState(0.0, 999.9, "X", epoch, None, "v2")
        # The correction the archive labels carried: the Offset Judge offset,
        # mirrored at top level for one release, otherwise under engineering.
        offset_ns = _opt_float(timing.get("offset_ns"))
        if offset_ns is None:
            offset_ns = _opt_float(eng.get("offset_ns"))
        u_ms = u_ns / 1e6
        return TimingState((offset_ns or 0.0) / 1e6, u_ms, grade_for(u_ms), epoch, origin, "v2")
    # Legacy: the Offset Judge verdict.  A verdict without an uncertainty
    # is not a verdict.
    sigma_ns = _opt_float(timing.get("offset_sigma_ns"))
    if sigma_ns is None:
        return UNKNOWN
    u_ms = sigma_ns / 1e6
    return TimingState(float(timing.get("offset_ns", 0.0)) / 1e6, u_ms, grade_for(u_ms), None, None, None)
```

In `decimation_pipeline.py`, replace the body of `timing_from_sidecar` (keep its docstring, add one line saying it delegates since 2026-09-04) with:

```python
    from hamsci_physics.grape.timing_state import timing_state_from_sidecar
    return timing_state_from_sidecar(meta).legacy_tuple
```

and replace the module-level `_GRADE_LADDER` / `UNKNOWN_TIMING` definitions with re-exports:

```python
from hamsci_physics.grape.timing_state import GRADE_LADDER as _GRADE_LADDER  # noqa: F401
from hamsci_physics.grape.timing_state import UNKNOWN as _UNKNOWN_STATE
UNKNOWN_TIMING = _UNKNOWN_STATE.legacy_tuple
```

- [ ] **Step 4: Run both test files**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_timing_state.py tests/test_grape_timing_metadata.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: all pass (the existing metadata tests are the legacy contract and must not change)

- [ ] **Step 5: Commit**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add src/hamsci_physics/grape/timing_state.py src/hamsci_physics/grape/decimation_pipeline.py tests/test_grape_timing_state.py tests/golden
git commit -m "grape: read the v2 timing state -- u_epoch_ns decides, the tier is never read; legacy blocks decode as before

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
```

---

### Task 2: `counter_epoch_id` and `origin` per minute; no extrapolation across an epoch change

**Files:**
- Modify: `src/hamsci_physics/grape/decimated_buffer.py` (`MinuteMetadata`, `DayMetadata.update_summary`/`to_dict`, `write_minute` signature)
- Modify: `src/hamsci_physics/grape/decimation_pipeline.py:~240-245` (pass the two fields)
- Test: `tests/test_grape_counter_epoch.py` (create)

**Interfaces:**
- `MinuteMetadata` gains `counter_epoch_id: Optional[str] = None`, `origin: Optional[str] = None` (defaulted, so old `_meta.json` files load).
- `DecimatedBuffer.write_minute(..., counter_epoch_id: Optional[str] = None, origin: Optional[str] = None)`.
- `DayMetadata.to_dict()["summary"]` gains `counter_epochs` (count of distinct non-null ids), `origin_switches` (count of minute-to-minute changes of a non-null origin), `epoch_boundaries` (list of minute indices where the id changed).
- `decimation_pipeline._process_channel_day` uses `timing_state_from_sidecar(meta)` and passes both fields. **The rule:** the decimator's state is reset (`StatefulDecimator(input_rate, 10)` re-created) at an epoch boundary, so no filter history spans a re-based counter. Log one INFO line per boundary.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_grape_counter_epoch.py
"""MEASUREMENT_MODEL §3: a radiod restart renumbers the samples; no consumer
extrapolates across a change in counter_epoch_id.  The day's metadata names
every boundary."""
import json
import tempfile
from pathlib import Path

import numpy as np

from hamsci_physics.grape.decimated_buffer import DayMetadata, DecimatedBuffer, MinuteMetadata, SAMPLES_PER_MINUTE


def test_minute_metadata_carries_epoch_and_origin_and_defaults_for_old_files():
    m = MinuteMetadata(minute_index=3, utc_timestamp=0.0, d_clock_ms=0.0, uncertainty_ms=1.0,
                       quality_grade="A", gap_samples=0, valid=True)
    assert m.counter_epoch_id is None and m.origin is None
    d = MinuteMetadata(**{**m.to_dict(), "counter_epoch_id": "pair-1", "origin": "sysclock"}).to_dict()
    assert d["counter_epoch_id"] == "pair-1" and d["origin"] == "sysclock"


def test_day_summary_counts_epochs_and_boundaries():
    day = DayMetadata(channel="SHARED_5000", date="2026-09-04")
    for i, (epoch, origin) in enumerate([("pair-1", "sysclock"), ("pair-1", "sysclock"),
                                         ("pair-2", "sysclock"), ("pair-2", "native_anchor")]):
        day.minutes[str(i)] = {"minute_index": i, "valid": True, "gap_samples": 0,
                               "counter_epoch_id": epoch, "origin": origin}
    s = day.to_dict()["summary"]
    assert s["counter_epochs"] == 2 and s["epoch_boundaries"] == [2]
    assert s["origin_switches"] == 1


def test_write_minute_records_the_fields():
    with tempfile.TemporaryDirectory() as d:
        buf = DecimatedBuffer(Path(d), "SHARED_5000")
        ok = buf.write_minute(minute_utc=1_788_566_400.0, decimated_iq=np.zeros(SAMPLES_PER_MINUTE, np.complex64),
                              d_clock_ms=0.0, uncertainty_ms=0.004, quality_grade="A", gap_samples=0,
                              counter_epoch_id="r-2026-09-04T16:03:35Z", origin="native_anchor")
        assert ok
        buf.flush_metadata()
        meta = json.loads(next(Path(d).rglob("*_meta.json")).read_text())
        (minute,) = meta["minutes"].values()
        assert minute["counter_epoch_id"] == "r-2026-09-04T16:03:35Z" and minute["origin"] == "native_anchor"


def test_pipeline_resets_the_decimator_at_an_epoch_boundary(monkeypatch):
    """A new StatefulDecimator is built when counter_epoch_id changes, so no
    filter history spans a re-based counter."""
    from hamsci_physics.grape import decimation_pipeline as dp
    built = []
    class FakeDecimator:
        def __init__(self, input_rate, output_rate):
            built.append((input_rate, output_rate))
        def process(self, samples):
            return np.zeros(600, np.complex64)
    monkeypatch.setattr(dp, "StatefulDecimator", FakeDecimator)
    epochs = iter(["pair-1", "pair-1", "pair-2"])
    class FakeReader:
        def __init__(self, *a, **k): pass
        def get_sample_rate(self, date_str): return 24000
        def get_available_minutes(self, date_str):
            base = 1_788_566_400
            return [base, base + 60, base + 120]
        def read_minute(self, ts):
            return (np.zeros(24000 * 60, np.complex64),
                    {"timing": {"schema": "v2", "origin": "sysclock", "u_epoch_ns": 8_030_000,
                                "counter_epoch_id": next(epochs)}})
    class FakeBuffer:
        def __init__(self, *a, **k): self.rows = []
        def write_minute(self, **kw): self.rows.append(kw); return True
        def flush_metadata(self): pass
    monkeypatch.setattr(dp, "RawBinaryReader", FakeReader)
    monkeypatch.setattr(dp, "DecimatedBuffer", FakeBuffer)
    dp.DecimationPipeline(Path("/nonexistent"))._process_channel_day("20260904", "SHARED_5000")
    assert len(built) == 2, "one decimator for pair-1, a fresh one for pair-2"
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_counter_epoch.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `TypeError: __init__() got an unexpected keyword argument 'counter_epoch_id'`

- [ ] **Step 3: Implement**

`decimated_buffer.py`:

```python
@dataclass
class MinuteMetadata:
    """Metadata for one minute of decimated data."""
    minute_index: int
    utc_timestamp: float
    d_clock_ms: float
    uncertainty_ms: float
    quality_grade: str
    gap_samples: int
    valid: bool
    # TIMING_PROVENANCE_MODEL §3.1 (2026-09-04): whose numbering the minute's
    # registration belongs to, and which registration method produced it.
    # None on products older than the v2 timing block.
    counter_epoch_id: Optional[str] = None
    origin: Optional[str] = None
```

`DayMetadata`: add fields `counter_epochs: int = 0`, `origin_switches: int = 0`, `epoch_boundaries: List[int] = field(default_factory=list)`; in `update_summary()` add:

```python
        ordered = sorted(self.minutes.values(), key=lambda m: int(m.get('minute_index', 0)))
        ids = [m.get('counter_epoch_id') for m in ordered]
        self.counter_epochs = len({i for i in ids if i})
        self.epoch_boundaries = [
            int(ordered[k]['minute_index']) for k in range(1, len(ordered))
            if ids[k] and ids[k - 1] and ids[k] != ids[k - 1]]
        origins = [m.get('origin') for m in ordered]
        self.origin_switches = sum(
            1 for k in range(1, len(origins))
            if origins[k] and origins[k - 1] and origins[k] != origins[k - 1])
```

and in `to_dict()['summary']` add `'counter_epochs': self.counter_epochs, 'origin_switches': self.origin_switches, 'epoch_boundaries': list(self.epoch_boundaries)`.

`write_minute(...)`: add parameters `counter_epoch_id: Optional[str] = None, origin: Optional[str] = None` and pass them into the `MinuteMetadata(...)` it constructs.

`decimation_pipeline._process_channel_day`: replace `d_clock, uncertainty, grade = timing_from_sidecar(meta)` with

```python
                state = timing_state_from_sidecar(meta)
                if state.counter_epoch_id and last_epoch and state.counter_epoch_id != last_epoch:
                    logger.info(f"  {channel_name}: counter epoch {last_epoch} -> {state.counter_epoch_id} "
                                f"at minute {minute_index}; resetting decimator state (MEASUREMENT_MODEL §3)")
                    decimator = StatefulDecimator(input_rate=input_rate, output_rate=10)
                if state.counter_epoch_id:
                    last_epoch = state.counter_epoch_id
```

placed BEFORE `decimator.process(samples)` for the minute (move the `timing_state_from_sidecar` call up to where `meta` becomes available), initialise `last_epoch = None` before the loop, import `timing_state_from_sidecar`, and pass `counter_epoch_id=state.counter_epoch_id, origin=state.origin` plus `state.d_clock_ms, state.uncertainty_ms, state.quality_grade` into `write_minute`.

- [ ] **Step 4: Run the grape tests**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests -p no:cacheprovider --override-ini="addopts=" -q -k "grape"`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add src/hamsci_physics/grape/decimated_buffer.py src/hamsci_physics/grape/decimation_pipeline.py tests/test_grape_counter_epoch.py
git commit -m "grape: record counter_epoch_id and origin per minute; a fresh decimator at every epoch boundary

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
```

---

### Task 3: The day's chain sidecar

**Files:**
- Create: `src/hamsci_physics/grape/timing_chain_sidecar.py`
- Test: `tests/test_grape_timing_chain_sidecar.py`

**Interfaces:**
- `STABLE_KEYS = ("chain", "origin", "counter_epoch_id", "a_level", "a_level_provenance")` — a `state` line is appended when these change (mag-recorder's policy), plus a heartbeat every `heartbeat_sec` (default 600).
- `write_day_sidecar(data_root: Path, date_str: str, *, chain_path: Path = Path("/run/hf-timestd/timing_chain.json"), out_dir: Optional[Path] = None, heartbeat_sec: float = 600.0, now_fn=time.time) -> Optional[Path]`: reads the chains file (if present) and every chunk sidecar JSON of the day under `data_root` (the same files `RawBinaryReader` reads; locate them with `RawBinaryReader(data_root, channel).get_available_minutes` per channel dir, or by globbing `*.json` beside `*.bin.zst` under the day's raw directories — follow how `RawBinaryReader` finds them), and writes `<out_dir or data_root/'timing'>/timing-chain-YYYYMMDD.jsonl` with: the `chain` records first (each once, plus once whenever a state record names a chain id not yet written), then state lines per the policy, one per channel. Returns the path, or None on any failure (logged).
- Chunk sidecars without a v2 `timing` block contribute nothing.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_grape_timing_chain_sidecar.py
"""timing-chain-YYYYMMDD.jsonl: chain records once, state lines on change
plus heartbeat (mag-recorder's policy), outside the PSWS payload."""
import json
from pathlib import Path

from hamsci_physics.grape.timing_chain_sidecar import STABLE_KEYS, write_day_sidecar

GOLDEN = Path(__file__).parent / "golden"


def _chunk(dirpath: Path, minute_ts: int, state: dict):
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / f"{minute_ts}.json").write_text(json.dumps({"timing": state}))


def test_sidecar_writes_chain_once_then_state_on_change_and_heartbeat(tmp_path):
    state = json.loads((GOLDEN / "timing_state_b4_20260904T1610Z.json").read_text())
    raw = tmp_path / "raw" / "T6_96000" / "20260904"
    base = 1_788_566_400
    for k in range(6):                                  # six 10-min chunks, one epoch change
        s = dict(state); s["t"] = f"2026-09-04T{k:02d}:00:00.000000Z"
        s["counter_epoch_id"] = "pair-1" if k < 4 else "pair-2"
        _chunk(raw, base + 600 * k, s)
    chains = tmp_path / "timing_chain.json"
    chains.write_text(json.dumps({"schema": "v2", "written_utc": "x",
                                  "chains": [json.loads((GOLDEN / "timing_chain_payload_anchored_v1.json").read_text())]}))
    out = write_day_sidecar(tmp_path, "20260904", chain_path=chains, heartbeat_sec=1800.0,
                            raw_glob="raw/*/20260904/*.json")
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    kinds = [l["type"] for l in lines]
    assert kinds[0] == "chain" and kinds.count("chain") == 1
    states = [l for l in lines if l["type"] == "state"]
    # first chunk (change from nothing), heartbeat at 30 min (k=3), epoch change at k=4
    assert [s["t"][11:13] for s in states] == ["00", "03", "04"]
    assert states[-1]["counter_epoch_id"] == "pair-2"
    assert out == tmp_path / "timing" / "timing-chain-20260904.jsonl"


def test_missing_chain_file_and_legacy_chunks_yield_an_empty_but_valid_file(tmp_path):
    raw = tmp_path / "raw" / "SHARED_5000" / "20260904"
    _chunk(raw, 1_788_566_400, {"offset_ns": 1.0, "offset_sigma_ns": 2.0, "judge_tier": "T4"})
    out = write_day_sidecar(tmp_path, "20260904", chain_path=tmp_path / "absent.json",
                            raw_glob="raw/*/20260904/*.json")
    assert out is not None and out.read_text() == ""


def test_never_raises(tmp_path):
    assert write_day_sidecar(Path("/nonexistent/root"), "20260904") is None
    assert STABLE_KEYS == ("chain", "origin", "counter_epoch_id", "a_level", "a_level_provenance")
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_timing_chain_sidecar.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/hamsci_physics/grape/timing_chain_sidecar.py
"""The day's chain sidecar: timing-chain-YYYYMMDD.jsonl.

TIMING_PROVENANCE_MODEL §3.0.1 / §3.2 — chain records change rarely and are
cross-chunk, so they get their own file under mag-recorder's policy: append
when the STABLE identity changes, plus a heartbeat to bound staleness.
State records already live per chunk; the sidecar carries only the
identity changes so a reader can see the day's shape without opening 144
chunk files.

Where it lands, and why not in the OBS payload (a recorded deviation from
§3.0.1): PSWS ingestion keys on payload convention.  mag-recorder learned
on 2026-08-21 that an extra file in the zip is stored and never ingested,
and GRAPE's PSWS server rejects unexpected DRF metadata.  So the file lands
at <data_root>/timing/, beside the OBS output, and is not uploaded until a
PSWS acceptance test licenses it.  Best-effort throughout.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_CHAIN_PATH = Path("/run/hf-timestd/timing_chain.json")
STABLE_KEYS = ("chain", "origin", "counter_epoch_id", "a_level", "a_level_provenance")


def _parse_t_sec(t: str) -> float:
    from datetime import datetime, timezone
    s = t[:-1] if t.endswith("Z") else t
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def write_day_sidecar(
    data_root: Path, date_str: str, *,
    chain_path: Path = DEFAULT_CHAIN_PATH, out_dir: Optional[Path] = None,
    heartbeat_sec: float = 600.0, raw_glob: Optional[str] = None,
    now_fn: Callable[[], float] = time.time,
) -> Optional[Path]:
    """Write the day's sidecar; return its path, or None on failure."""
    try:
        data_root = Path(data_root)
        date_str = date_str.replace("-", "")
        out_dir = Path(out_dir) if out_dir is not None else data_root / "timing"
        pattern = raw_glob or f"raw/*/{date_str}/*.json"
        states = []
        for p in sorted(data_root.glob(pattern)):
            try:
                timing = (json.loads(p.read_text()) or {}).get("timing") or {}
            except (OSError, ValueError):
                continue
            if timing.get("schema") == "v2" and timing.get("t"):
                states.append(timing)
        states.sort(key=lambda s: (s.get("counter_space", ""), s["t"]))

        chains_by_id = {}
        try:
            doc = json.loads(Path(chain_path).read_text())
            for c in doc.get("chains") or []:
                chains_by_id[c.get("id")] = c
        except (OSError, ValueError):
            logger.info("timing chain file %s unavailable; sidecar carries state only", chain_path)

        lines = []
        written_chains = set()

        def _emit_chain(cid):
            if cid in chains_by_id and cid not in written_chains:
                lines.append(chains_by_id[cid])
                written_chains.add(cid)

        for cid in chains_by_id:
            _emit_chain(cid)

        last_stable = {}
        last_write_t = {}
        for s in states:
            space = s.get("counter_space", "")
            stable = tuple(s.get(k) for k in STABLE_KEYS)
            t = _parse_t_sec(s["t"])
            changed = stable != last_stable.get(space)
            due = space not in last_write_t or t - last_write_t[space] >= heartbeat_sec
            if not changed and not due:
                continue
            _emit_chain(s.get("chain"))
            lines.append({k: s.get(k) for k in ("t", "type", "schema", "counter_space", *STABLE_KEYS,
                                                 "u_epoch_ns", "k", "p", "measured_at", "stability_ns", "tau_s", "reason")})
            last_stable[space] = stable
            last_write_t[space] = t

        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"timing-chain-{date_str}.jsonl"
        tmp = out.with_suffix(".jsonl.tmp")
        tmp.write_text("".join(json.dumps(l, separators=(",", ":")) + "\n" for l in lines))
        tmp.replace(out)
        logger.info("timing chain sidecar: %s (%d chain, %d state lines)", out,
                    len(written_chains), len(lines) - len(written_chains))
        return out
    except Exception:  # noqa: BLE001 — the sidecar never kills the pipeline
        logger.exception("timing chain sidecar: failed (continuing)")
        return None
```

Check the raw layout `RawBinaryReader` uses (`grep -n "glob\|def get_available_minutes\|\.json" src/hamsci_physics/grape/raw_reader.py`) and set the default `pattern` to match the real directory shape (the test passes its own `raw_glob`).

- [ ] **Step 4: Run to verify pass**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_timing_chain_sidecar.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add src/hamsci_physics/grape/timing_chain_sidecar.py tests/test_grape_timing_chain_sidecar.py
git commit -m "grape: the day's timing-chain sidecar, mag-recorder's change-plus-heartbeat policy, outside the PSWS payload

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
```

---

### Task 4: The overclaim gate

**Files:**
- Create: `src/hamsci_physics/grape/overclaim_gate.py`
- Test: `tests/test_grape_overclaim_gate.py`

**Interfaces:**
- `IntervalVerdict(counter_space: str, t_start: str, t_end: str, origin: Optional[str], published_u_ms: Optional[float], k: int, observed_ms: Optional[float], witness: Optional[str], n: int, overclaim: bool, reason: str)`
- `GateReport(date: str, intervals: list[IntervalVerdict], overclaims: int, unwitnessed: int)`; `.to_dict()`.
- `assess_day(state_records: list[dict], history_rows: list[dict]) -> GateReport`:
  - For each state record (one interval per chunk, from its `t` to the next record's `t` in the same `counter_space`, or +600 s for the last): select history rows with `utc_published` in the interval.
  - `origin == "native_anchor"`: witness `T5` — observed = RMS of `t5_offset_ms` over rows where `t5_available == 1`; **fail** if `k * published_u_ms < observed`.
  - `origin == "sysclock"`: witness `host_clock_t2_ms` — observed = max |`host_clock_t2_ms`| over rows; fail if `k * published_u_ms < observed`. (The pair's p99 stand-in versus the witnessed disagreement — MEASUREMENT_MODEL §9 invariant 5.)
  - `origin None` or no rows with the witness: `unwitnessed`, never a failure.
- `load_history_rows(db_path: Path, day_start_iso: str, day_end_iso: str) -> list[dict]` — sqlite3 read of `authority_snapshot` columns `utc_published, t5_available, t5_offset_ms, host_clock_t2_ms, host_clock_verdict`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_grape_overclaim_gate.py
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_overclaim_gate.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/hamsci_physics/grape/overclaim_gate.py
"""The overclaim gate (TIMING_PROVENANCE_MODEL §8; MEASUREMENT_MODEL §9
invariant 5): the published uncertainty never falls below the scatter an
independent registration observed over the same interval.

Witnesses, by origin, chosen so the comparison stays inside one frame
(METROLOGY §4.5 forbids cross-frame comparison):
  native_anchor  -> T5 (LB-1421 direct, rtp frame): RMS of t5_offset_ms,
                    the anchor's disagreement with GPS/NMEA truth.
  sysclock       -> the host-clock verdict's T2 pair witness: the label
                    descends from the host clock, so a claim narrower than
                    what T2 saw is exactly the record 2026-09-04 lacked.
No witness rows -> unwitnessed, never a failure.  Absence never overclaims.
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _sec(t: str) -> float:
    s = t[:-1] if t.endswith("Z") else t
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


@dataclass(frozen=True)
class IntervalVerdict:
    counter_space: str
    t_start: str
    t_end: str
    origin: Optional[str]
    published_u_ms: Optional[float]
    k: int
    observed_ms: Optional[float]
    witness: Optional[str]
    n: int
    overclaim: bool
    reason: str


@dataclass
class GateReport:
    date: str
    intervals: List[IntervalVerdict] = field(default_factory=list)
    overclaims: int = 0
    unwitnessed: int = 0

    def to_dict(self) -> dict:
        return {"date": self.date, "overclaims": self.overclaims, "unwitnessed": self.unwitnessed,
                "intervals": [asdict(v) for v in self.intervals]}


def _rms(xs: List[float]) -> float:
    return math.sqrt(sum(x * x for x in xs) / len(xs))


def assess_day(state_records: List[dict], history_rows: List[dict]) -> GateReport:
    states = sorted((s for s in state_records if s.get("t")), key=lambda s: (s.get("counter_space", ""), s["t"]))
    rows = sorted((r for r in history_rows if r.get("utc_published")), key=lambda r: r["utc_published"])
    row_t = [_sec(r["utc_published"]) for r in rows]
    date = states[0]["t"][:10] if states else ""
    rep = GateReport(date=date)
    for i, s in enumerate(states):
        space = s.get("counter_space", "")
        t0 = _sec(s["t"])
        nxt = next((x for x in states[i + 1:] if x.get("counter_space", "") == space), None)
        t1 = _sec(nxt["t"]) if nxt else t0 + 600.0
        in_window = [r for r, t in zip(rows, row_t) if t0 <= t < t1]
        origin = s.get("origin")
        u_ns = s.get("u_epoch_ns")
        k = int(s.get("k") or 1)
        t_start, t_end = s["t"], datetime.fromtimestamp(t1, tz=timezone.utc).isoformat().replace("+00:00", "Z")

        def _v(observed, witness, n, overclaim, reason, u_ms):
            return IntervalVerdict(space, t_start, t_end, origin, u_ms, k, observed, witness, n, overclaim, reason)

        if origin is None or u_ns is None:
            rep.unwitnessed += 1
            rep.intervals.append(_v(None, None, 0, False, "absence: no registration published", None))
            continue
        u_ms = float(u_ns) / 1e6
        if origin == "native_anchor":
            vals = [float(r["t5_offset_ms"]) for r in in_window
                    if r.get("t5_available") == 1 and r.get("t5_offset_ms") is not None]
            witness = "T5"
            observed = _rms(vals) if vals else None
        else:
            vals = [abs(float(r["host_clock_t2_ms"])) for r in in_window if r.get("host_clock_t2_ms") is not None]
            witness = "host_clock_t2_ms"
            observed = max(vals) if vals else None
        if observed is None:
            rep.unwitnessed += 1
            rep.intervals.append(_v(None, witness, 0, False, f"unwitnessed: no {witness} rows in the interval", u_ms))
            continue
        claim = k * u_ms
        overclaim = claim < observed
        if overclaim:
            rep.overclaims += 1
        rep.intervals.append(_v(observed, witness, len(vals), overclaim,
                                (f"OVERCLAIM: k*u = {claim:.4f} ms < observed {observed:.4f} ms ({witness}, n={len(vals)})"
                                 if overclaim else f"ok: k*u = {claim:.4f} ms >= observed {observed:.4f} ms ({witness}, n={len(vals)})"),
                                u_ms))
    return rep


def load_history_rows(db_path: Path, day_start_iso: str, day_end_iso: str) -> List[dict]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            "SELECT utc_published, t5_available, t5_offset_ms, host_clock_t2_ms, host_clock_verdict "
            "FROM authority_snapshot WHERE utc_published >= ? AND utc_published < ? ORDER BY utc_published",
            (day_start_iso, day_end_iso))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
```

- [ ] **Step 4: Run to verify pass**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_grape_overclaim_gate.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add src/hamsci_physics/grape/overclaim_gate.py tests/test_grape_overclaim_gate.py
git commit -m "grape: the overclaim gate -- a published uncertainty never falls below the scatter an independent registration observed

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
```

---

### Task 5: CLI commands and the daily pipeline

**Files:**
- Modify: `src/hamsci_physics/cli.py` (the `grape` subparsers block near L166-221; the dispatch that runs `grape_command`)
- Modify: the daily pipeline module (locate: `grep -rn "def run_daily\|def daily\|package_day(" src/hamsci_physics/grape/*.py | head`)
- Test: `tests/test_cli_timing_commands.py` (create)

**Interfaces:**
- `hamsci-physics grape timing-chain --data-root PATH --date YYYYMMDD [--chain-path PATH] [--out-dir PATH]` → prints the written path; exit 0 even when the sidecar is empty; exit 1 only when it returns None.
- `hamsci-physics grape timing-gate --data-root PATH --date YYYYMMDD [--history-db /var/lib/timestd/authority_history.db] [--json]` → prints the report; exit 1 when `overclaims > 0`, else 0.
- Daily pipeline: after packaging, call `write_day_sidecar(data_root, date)` and run the gate; log `overclaims` and `unwitnessed`; never fail the pipeline on either.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_cli_timing_commands.py
import json

from hamsci_physics.cli import build_parser


def test_timing_chain_and_gate_parse():
    p = build_parser()
    a = p.parse_args(["grape", "timing-chain", "--data-root", "/d", "--date", "20260904"])
    assert (a.command, a.grape_command, a.date) == ("grape", "timing-chain", "20260904")
    a = p.parse_args(["grape", "timing-gate", "--data-root", "/d", "--date", "20260904",
                      "--history-db", "/tmp/h.db", "--json"])
    assert a.grape_command == "timing-gate" and a.json is True and a.history_db == "/tmp/h.db"


def test_timing_gate_exit_code_follows_overclaims(tmp_path, capsys):
    import sqlite3
    from hamsci_physics.cli import run_timing_gate
    db = tmp_path / "h.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE authority_snapshot (utc_published TEXT PRIMARY KEY, t5_available INTEGER, "
                 "t5_offset_ms REAL, host_clock_t2_ms REAL, host_clock_verdict TEXT)")
    for i in range(6):
        conn.execute("INSERT INTO authority_snapshot VALUES (?,?,?,?,?)",
                     (f"2026-09-04T16:0{i}:00Z", 1, (-1.0) ** i * 1.2, None, "ok"))
    conn.commit(); conn.close()
    raw = tmp_path / "raw" / "T6_96000" / "20260904"; raw.mkdir(parents=True)
    (raw / "1788566400.json").write_text(json.dumps({"timing": {
        "t": "2026-09-04T16:00:00.000000Z", "type": "state", "schema": "v2",
        "counter_space": "x/T6_96000", "counter_epoch_id": "r-1", "origin": "native_anchor",
        "u_epoch_ns": 4093, "k": 2}}))
    rc = run_timing_gate(tmp_path, "20260904", db, as_json=True, raw_glob="raw/*/20260904/*.json")
    out = json.loads(capsys.readouterr().out)
    assert rc == 1 and out["overclaims"] == 1
```

(If `cli.py` has no `build_parser`, locate the parser construction and factor it into `build_parser()` as part of this task; the existing `main()` calls it.)

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests/test_cli_timing_commands.py -p no:cacheprovider --override-ini="addopts=" -q`
Expected: `error: argument grape_command: invalid choice: 'timing-chain'` (or `ImportError` for `run_timing_gate`)

- [ ] **Step 3: Implement**

In `cli.py`, after the `grape status` subparser:

```python
    tc = grape_subparsers.add_parser('timing-chain', help='Write the day\'s timing-chain sidecar (TIMING_PROVENANCE_MODEL §3.2)')
    tc.add_argument('--data-root', type=Path, required=True)
    tc.add_argument('--date', type=str, required=True, help='YYYYMMDD')
    tc.add_argument('--chain-path', type=Path, default=Path('/run/hf-timestd/timing_chain.json'))
    tc.add_argument('--out-dir', type=Path, default=None)

    tg = grape_subparsers.add_parser('timing-gate', help='Overclaim gate: published u_epoch vs observed scatter (spec §8)')
    tg.add_argument('--data-root', type=Path, required=True)
    tg.add_argument('--date', type=str, required=True, help='YYYYMMDD')
    tg.add_argument('--history-db', type=str, default='/var/lib/timestd/authority_history.db')
    tg.add_argument('--json', action='store_true')
```

and the two runners plus dispatch:

```python
def run_timing_chain(data_root: Path, date: str, chain_path: Path, out_dir) -> int:
    from hamsci_physics.grape.timing_chain_sidecar import write_day_sidecar
    out = write_day_sidecar(data_root, date, chain_path=chain_path, out_dir=out_dir)
    if out is None:
        print("FAILED: timing chain sidecar not written", file=sys.stderr)
        return 1
    print(str(out))
    return 0


def run_timing_gate(data_root: Path, date: str, history_db, *, as_json: bool = False,
                    raw_glob: str = None) -> int:
    import json as _json
    from datetime import datetime, timedelta, timezone
    from hamsci_physics.grape.overclaim_gate import assess_day, load_history_rows
    date = date.replace('-', '')
    day = datetime.strptime(date, '%Y%m%d').replace(tzinfo=timezone.utc)
    start, end = day.isoformat().replace('+00:00', 'Z'), (day + timedelta(days=1)).isoformat().replace('+00:00', 'Z')
    pattern = raw_glob or f"raw/*/{date}/*.json"
    states = []
    for p in sorted(Path(data_root).glob(pattern)):
        try:
            t = (_json.loads(p.read_text()) or {}).get('timing') or {}
        except (OSError, ValueError):
            continue
        if t.get('schema') == 'v2' and t.get('t'):
            states.append(t)
    rows = load_history_rows(Path(history_db), start, end) if Path(history_db).exists() else []
    rep = assess_day(states, rows)
    if as_json:
        print(_json.dumps(rep.to_dict(), indent=1))
    else:
        print(f"{rep.date}: {len(rep.intervals)} intervals, {rep.overclaims} overclaim(s), {rep.unwitnessed} unwitnessed")
        for v in rep.intervals:
            if v.overclaim:
                print(f"  {v.counter_space} {v.t_start}: {v.reason}")
    return 1 if rep.overclaims else 0
```

Dispatch (in the `grape` branch of `main()`): `'timing-chain'` → `return run_timing_chain(args.data_root, args.date, args.chain_path, args.out_dir)`; `'timing-gate'` → `return run_timing_gate(args.data_root, args.date, args.history_db, as_json=args.json)`.

Daily pipeline: after the package step succeeds, add

```python
        try:
            from hamsci_physics.grape.timing_chain_sidecar import write_day_sidecar
            from hamsci_physics.cli import run_timing_gate
            write_day_sidecar(self.data_root, date_str)
            rc = run_timing_gate(self.data_root, date_str, '/var/lib/timestd/authority_history.db')
            logger.info("overclaim gate: %s", "OVERCLAIM(S) FOUND — see `hamsci-physics grape timing-gate`" if rc else "clean")
        except Exception as exc:  # noqa: BLE001 — provenance never fails the product
            logger.warning("timing sidecar/gate step failed (continuing): %s", exc)
```

- [ ] **Step 4: Run the whole suite**

Run: `cd /home/mjh/hamsci/repos/hamsci-physics && .venv/bin/python -m pytest tests -p no:cacheprovider --override-ini="addopts=" -q`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add src/hamsci_physics/cli.py src/hamsci_physics/grape tests/test_cli_timing_commands.py
git commit -m "cli: grape timing-chain and grape timing-gate; the daily pipeline runs both, report-only

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
```

---

### Task 6: `docs/TIMING_STATE.md` and the upload licensing condition

**Files:**
- Create: `docs/TIMING_STATE.md`
- Modify: `docs/INDEX.md` (one line under GRAPE)

- [ ] **Step 1: Write the page.** Sections, each a short paragraph: what the consumer reads (`u_epoch_ns`, `stability_ns`, `counter_epoch_id`, `origin`) and what it never reads (`judge_tier`); the grade ladder's scope; the epoch rule and the decimator reset; where the chain sidecar lands and why it is not in the PSWS payload, naming the licensing condition — "a PSWS acceptance test in which an OBS containing `timing-chain-<date>.jsonl` is ingested and the file survives; until then `include_in_upload` stays false"; the overclaim gate: what it compares, which witness per origin, and that the daily pipeline runs it report-only while the CLI exits 1. Cite `TIMING_PROVENANCE_MODEL.md` §3.0.1, §3.1, §8 and `MEASUREMENT_MODEL.md` §3, §8, §9.

- [ ] **Step 2: Commit and push**

```bash
cd /home/mjh/hamsci/repos/hamsci-physics
git add docs/TIMING_STATE.md docs/INDEX.md
git commit -m "docs: TIMING_STATE.md -- what GRAPE reads, the epoch rule, the sidecar's place and its upload condition, the gate

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_014fxKGYhGpcPpYFsPbDj4KH"
git push origin main
```

---

## Self-review

**Spec coverage.** §3.0 (extend the per-chunk block, keep the ladder, demote the tier) → Task 1; §3.0.1 (chain sidecar, change-plus-heartbeat) → Task 3, with the payload deviation recorded in Global Constraints and Task 6; §3.1 `counter_epoch_id` rule → Task 2; §3.4 (consumers read the normative subset) → Task 1; §6 deliverables 3–5 → Tasks 3, 1–2, 4–5; §7 absence → Task 1 (`origin None` → sentinels) and Task 4 (absence never overclaims); §8 overclaim gate → Task 4; MEASUREMENT_MODEL §9 invariant 5 → Task 4; §3 re-basing → Task 2.

**Placeholders.** Two locating instructions name a `grep` because the target file's line numbers were not read for this plan (`raw_reader` glob shape, daily pipeline entry point); each names the anchor and gives the code. `build_parser()` may need factoring in Task 5; the step says so and what to do.

**Type consistency.** `TimingState` fields match between Task 1 (definition), Task 2 (`state.counter_epoch_id`, `state.origin`, `legacy_tuple`) and Task 5; `write_day_sidecar(data_root, date_str, *, chain_path, out_dir, heartbeat_sec, raw_glob, now_fn)` matches its three call sites; `assess_day(state_records, history_rows)` and `load_history_rows(db_path, start, end)` match Task 5; history column names match hamsci-dsp Plan A Task 5 and hf-timestd Plan B Task 1.
