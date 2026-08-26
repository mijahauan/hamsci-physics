"""GRAPE upload-spool retention: age AND proof of delivery.

The safety property under test is the one the 2026-08-24→26 outage taught us:
uploads can be dead for days while every status surface reads green.  A trim
that deletes on age alone would, in a long enough outage, silently destroy
science that was never delivered.  So "old" must never be sufficient.

``spool`` is loaded straight off disk so these run without numpy/h5py/
hs_uploader present.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

import pytest

_MOD = (Path(__file__).resolve().parents[1]
        / "src" / "hamsci_physics" / "grape" / "spool.py")
_spec = importlib.util.spec_from_file_location("spool", _MOD)
spool = importlib.util.module_from_spec(_spec)
# @dataclass resolves its module via sys.modules, so a file-loaded module has
# to be registered before exec.
sys.modules["spool"] = spool
_spec.loader.exec_module(spool)

NS_PER_DAY = 86_400 * 1_000_000_000
NOW = 1_800_000_000 * 1_000_000_000


def make_package(root: Path, date: str, *, age_days: float) -> Path:
    """A realistic <date>/<call_grid>/GRAPE@.../OBS<date>T00-00 tree."""
    obs = (root / date / "AC0G_EM38ww" / "GRAPE@AC0G_1_1"
           / f"OBS{date[:4]}-{date[4:6]}-{date[6:]}T00-00")
    (obs / "ch0").mkdir(parents=True)
    (obs / "ch0" / "drf_properties.h5").write_bytes(b"x" * 32)
    (obs / "gap_summary.json").write_text('{"date": "x"}')
    mtime_ns = int(NOW - age_days * NS_PER_DAY)
    os.utime(obs, ns=(mtime_ns, mtime_ns))
    return root / date


def obs_mtime_ns(package: Path) -> int:
    return max(p.stat().st_mtime_ns
               for p in package.rglob("OBS*") if p.is_dir())


class TestTrimSafety:
    def test_old_but_unshipped_is_kept_and_reported(self, tmp_path):
        """The outage case.  Deleting this would be data loss."""
        pkg = make_package(tmp_path, "20260801", age_days=40)
        # cursor is BEHIND the package => never acked
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(pkg) - 1,
                               max_age_days=14, now_ns=NOW)
        assert res.removed == []
        assert res.kept_unshipped == [pkg]
        assert pkg.exists()
        assert any("NOT confirmed shipped" in ln for ln in res.summary_lines())

    def test_unknown_cursor_deletes_nothing(self, tmp_path):
        """No proof of delivery => no deletions, however old."""
        pkg = make_package(tmp_path, "20260801", age_days=400)
        res = spool.trim_spool(tmp_path, cursor_ns=None,
                               max_age_days=14, now_ns=NOW)
        assert res.removed == []
        assert pkg.exists()
        assert not res.cursor_known
        assert any("cursor unreadable" in ln for ln in res.summary_lines())

    def test_shipped_but_recent_is_kept(self, tmp_path):
        pkg = make_package(tmp_path, "20260820", age_days=3)
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(pkg),
                               max_age_days=14, now_ns=NOW)
        assert res.removed == []
        assert res.kept_recent == 1
        assert pkg.exists()

    def test_shipped_and_aged_out_is_removed(self, tmp_path):
        pkg = make_package(tmp_path, "20260801", age_days=40)
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(pkg),
                               max_age_days=14, now_ns=NOW)
        assert res.removed == [pkg]
        assert not pkg.exists()
        assert res.bytes_freed > 0

    def test_boundary_exactly_at_the_window_is_kept(self, tmp_path):
        pkg = make_package(tmp_path, "20260812", age_days=14)
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(pkg),
                               max_age_days=14, now_ns=NOW)
        assert res.removed == []
        assert pkg.exists()

    def test_mixed_spool_trims_only_what_qualifies(self, tmp_path):
        old_shipped = make_package(tmp_path, "20260701", age_days=60)
        old_unshipped = make_package(tmp_path, "20260801", age_days=40)
        recent = make_package(tmp_path, "20260825", age_days=1)
        # cursor acked the oldest only
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(old_shipped),
                               max_age_days=14, now_ns=NOW)
        assert res.removed == [old_shipped]
        assert res.kept_unshipped == [old_unshipped]
        assert res.kept_recent == 1
        assert not old_shipped.exists()
        assert old_unshipped.exists() and recent.exists()

    def test_dry_run_reports_without_deleting(self, tmp_path):
        pkg = make_package(tmp_path, "20260801", age_days=40)
        res = spool.trim_spool(tmp_path, cursor_ns=obs_mtime_ns(pkg),
                               max_age_days=14, now_ns=NOW, dry_run=True)
        assert res.removed == [pkg]
        assert pkg.exists()

    def test_dir_without_an_obs_dataset_is_never_touched(self, tmp_path):
        stray = tmp_path / "20260101"
        stray.mkdir()
        (stray / "notes.txt").write_text("hi")
        os.utime(stray, ns=(NOW - 400 * NS_PER_DAY,) * 2)
        res = spool.trim_spool(tmp_path, cursor_ns=NOW,
                               max_age_days=14, now_ns=NOW)
        assert res.removed == []
        assert stray.exists()

    def test_missing_root_is_not_an_error(self, tmp_path):
        res = spool.trim_spool(tmp_path / "nope", cursor_ns=NOW, now_ns=NOW)
        assert res.removed == []


class TestCursorRead:
    def _db(self, tmp_path, rows):
        db = tmp_path / "watermarks.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE watermarks (source_id TEXT, dest_id TEXT, "
                     "table_name TEXT, cursor BLOB, last_ack TEXT)")
        conn.executemany("INSERT INTO watermarks VALUES (?,?,?,?,?)", rows)
        conn.commit()
        conn.close()
        return db

    def test_reads_the_live_schema(self, tmp_path):
        """Row shape copied verbatim from B4's watermarks.db 2026-08-26."""
        db = self._db(tmp_path, [(
            "grape-datasets:/var/lib/timestd/upload",
            "psws-grape-sftp:pswsnetwork.eng.ua.edu:S000170",
            "grape.dataset", b"1787724854957426217",
            "2026-08-26T18:52:32+00:00")])
        assert spool.shipped_cursor_ns(db) == 1787724854957426217

    def test_legacy_float_seconds_cursor(self, tmp_path):
        db = self._db(tmp_path, [(
            spool.DEFAULT_SOURCE_ID, "d", spool.DEFAULT_TABLE,
            b"1781583014.421470", "")])
        # float64 cannot hold ns precision; sub-microsecond drift on a legacy
        # cursor is irrelevant to a 14-day retention decision.
        assert abs(spool.shipped_cursor_ns(db) - 1781583014421470000) < 1000

    def test_lowest_cursor_wins_across_destinations(self, tmp_path):
        db = self._db(tmp_path, [
            (spool.DEFAULT_SOURCE_ID, "d1", spool.DEFAULT_TABLE, b"200", ""),
            (spool.DEFAULT_SOURCE_ID, "d2", spool.DEFAULT_TABLE, b"100", ""),
        ])
        assert spool.shipped_cursor_ns(db) == 100

    def test_dest_id_filter(self, tmp_path):
        db = self._db(tmp_path, [
            (spool.DEFAULT_SOURCE_ID, "d1", spool.DEFAULT_TABLE, b"200", ""),
            (spool.DEFAULT_SOURCE_ID, "d2", spool.DEFAULT_TABLE, b"100", ""),
        ])
        assert spool.shipped_cursor_ns(db, dest_id="d1") == 200

    @pytest.mark.parametrize("rows", [[], [
        ("other-source", "d", "other.table", b"5", "")]])
    def test_no_matching_row_is_unknown_not_zero(self, tmp_path, rows):
        """Unknown must be None (=> keep everything), never 0 (=> nothing shipped
        is indistinguishable from an empty cursor, but both must be safe)."""
        assert spool.shipped_cursor_ns(self._db(tmp_path, rows)) is None

    def test_missing_db_is_unknown(self, tmp_path):
        assert spool.shipped_cursor_ns(tmp_path / "absent.db") is None

    def test_garbage_cursor_is_unknown(self, tmp_path):
        db = self._db(tmp_path, [(
            spool.DEFAULT_SOURCE_ID, "d", spool.DEFAULT_TABLE, b"not-a-number", "")])
        assert spool.shipped_cursor_ns(db) is None

    def test_read_is_read_only(self, tmp_path):
        """grape-daily must never be able to write the daemon's cursor store."""
        db = self._db(tmp_path, [(
            spool.DEFAULT_SOURCE_ID, "d", spool.DEFAULT_TABLE, b"7", "")])
        before = db.stat().st_mtime_ns
        assert spool.shipped_cursor_ns(db) == 7
        assert db.stat().st_mtime_ns == before


def test_default_window_is_fourteen_days():
    assert spool.DEFAULT_MAX_AGE_DAYS == 14
