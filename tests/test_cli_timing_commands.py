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
    raw = tmp_path / "raw_buffer" / "T6_96000" / "20260904"; raw.mkdir(parents=True)
    (raw / "1788537600.json").write_text(json.dumps({"timing": {
        "t": "2026-09-04T16:00:00.000000000Z", "type": "state", "schema": "v2",
        "counter_space": "x/T6_96000", "counter_epoch_id": "r-1", "origin": "native_anchor",
        "u_epoch_ns": 4093, "k": 2}}))
    rc = run_timing_gate(tmp_path, "20260904", db, as_json=True)
    out = json.loads(capsys.readouterr().out)
    assert rc == 1 and out["overclaims"] == 1


def test_timing_chain_command_writes_and_reports_the_path(tmp_path, capsys):
    from hamsci_physics.cli import run_timing_chain
    rc = run_timing_chain(tmp_path, "20260904", tmp_path / "absent.json", None)
    assert rc == 0
    assert capsys.readouterr().out.strip().endswith("timing/timing-chain-20260904.jsonl")
