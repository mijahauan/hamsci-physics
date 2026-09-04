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
from typing import Callable, Iterable, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_CHAIN_PATH = Path("/run/hf-timestd/timing_chain.json")
STABLE_KEYS = ("chain", "origin", "counter_epoch_id", "a_level", "a_level_provenance")
#: Where RawBinaryReader finds a day's chunk sidecars (cold and legacy
#: tiers; the hot tier under /dev/shm has migrated by the time the daily
#: pipeline runs for yesterday).
RAW_TIERS = ("raw_buffer", "raw_archive")


def _parse_t_sec(t: str) -> float:
    from datetime import datetime, timezone
    s = t[:-1] if t.endswith("Z") else t
    if "." in s:                                   # any number of fractional digits
        head, frac = s.split(".", 1)
        s = f"{head}.{(frac + '000000')[:6]}"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _sidecar_paths(data_root: Path, date_str: str, raw_glob: Optional[str]) -> Iterable[Path]:
    if raw_glob:
        yield from sorted(data_root.glob(raw_glob))
        return
    # No de-duplication across tiers: a chunk present in two tiers (mid
    # migration) yields two identical states, and the change-plus-heartbeat
    # policy writes at most one of them.
    for tier in RAW_TIERS:
        yield from sorted(data_root.glob(f"{tier}/*/{date_str}/*.json"))


def load_day_states(data_root: Path, date_str: str, raw_glob: Optional[str] = None) -> List[dict]:
    """Every v2 state record among the day's chunk sidecars, sorted by
    (counter_space, t).  Legacy sidecars contribute nothing."""
    states = []
    for p in _sidecar_paths(Path(data_root), date_str.replace("-", ""), raw_glob):
        try:
            timing = (json.loads(p.read_text()) or {}).get("timing") or {}
        except (OSError, ValueError):
            continue
        if timing.get("schema") == "v2" and timing.get("t"):
            states.append(timing)
    states.sort(key=lambda s: (s.get("counter_space", ""), s["t"]))
    return states


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
        states = load_day_states(data_root, date_str, raw_glob)

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
