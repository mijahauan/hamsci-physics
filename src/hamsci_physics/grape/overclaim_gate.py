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
    if "." in s:                                   # any number of fractional digits
        head, frac = s.split(".", 1)
        s = f"{head}.{(frac + '000000')[:6]}"
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
