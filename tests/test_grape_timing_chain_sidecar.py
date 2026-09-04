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
    state = json.loads((GOLDEN / "timing_state_b4_20260904T1600Z.json").read_text())
    raw = tmp_path / "raw_buffer" / "T6_96000" / "20260904"
    base = 1_788_566_400
    for k in range(6):                                  # six 10-min chunks, one epoch change
        s = dict(state); s["t"] = f"2026-09-04T00:{10 * k:02d}:00.000000000Z"   # 10-min chunks
        s["counter_epoch_id"] = "pair-1" if k < 4 else "pair-2"
        _chunk(raw, base + 600 * k, s)
    chains = tmp_path / "timing_chain.json"
    chains.write_text(json.dumps({"schema": "v2", "written_utc": "x",
                                  "chains": [json.loads((GOLDEN / "timing_chain_payload_anchored_v1.json").read_text())]}))
    out = write_day_sidecar(tmp_path, "20260904", chain_path=chains, heartbeat_sec=1800.0)
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    kinds = [l["type"] for l in lines]
    assert kinds[0] == "chain" and kinds.count("chain") == 1
    states = [l for l in lines if l["type"] == "state"]
    # first chunk (change from nothing), heartbeat at 30 min (k=3), epoch change at k=4
    assert [s["t"][14:16] for s in states] == ["00", "30", "40"]
    assert states[-1]["counter_epoch_id"] == "pair-2"
    assert out == tmp_path / "timing" / "timing-chain-20260904.jsonl"


def test_default_glob_matches_the_raw_reader_layout(tmp_path):
    """RawBinaryReader searches raw_buffer/<channel>/<date>/ (cold) and
    raw_archive/... (legacy); the sidecar reads the same sidecar files."""
    state = json.loads((GOLDEN / "timing_state_b4_20260904T1600Z.json").read_text())
    _chunk(tmp_path / "raw_buffer" / "SHARED_5000" / "20260904", 1_788_566_400, state)
    _chunk(tmp_path / "raw_archive" / "SHARED_10000" / "20260904", 1_788_566_400,
           {**state, "counter_space": "x/SHARED_10000"})
    out = write_day_sidecar(tmp_path, "2026-09-04", chain_path=tmp_path / "absent.json")
    states = [json.loads(l) for l in out.read_text().splitlines() if '"state"' in l]
    assert len(states) == 2


def test_missing_chain_file_and_legacy_chunks_yield_an_empty_but_valid_file(tmp_path):
    raw = tmp_path / "raw_buffer" / "SHARED_5000" / "20260904"
    _chunk(raw, 1_788_566_400, {"offset_ns": 1.0, "offset_sigma_ns": 2.0, "judge_tier": "T4"})
    out = write_day_sidecar(tmp_path, "20260904", chain_path=tmp_path / "absent.json")
    assert out is not None and out.read_text() == ""


def test_never_raises(tmp_path):
    assert write_day_sidecar(Path("/nonexistent/root"), "20260904") is None
    assert STABLE_KEYS == ("chain", "origin", "counter_epoch_id", "a_level", "a_level_provenance")
