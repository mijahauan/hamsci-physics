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
