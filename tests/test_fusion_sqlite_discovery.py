"""Channel discovery and upstream freshness must read the SQLite store.

Both predate the HDF5→SQLite migration and still read the file tree:

* ``_discover_channels`` looks for ``phase2/<channel>/clock_offset/``
  *directories*.  On B4 those directories still exist but hold zero
  ``.h5`` files, so discovery works purely by accident — prune the empty
  leftovers and the service discovers nothing and processes nothing.
* ``_check_upstream_freshness`` scans those directories for ``*.h5``,
  finds none, and therefore reports the upstream permanently stale.  It
  does not gate the science (the loop continues either way), but it is a
  health signal that always lies, which is worse than no signal during a
  cutover soak.

The measurements themselves have been in ``L2_timing_measurements``
(columns ``channel``, ``timestamp_utc``) since the migration.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hamsci_physics.physics_fusion_service import PhysicsFusionService


def _make_store(tmp_path: Path, rows) -> Path:
    """Build a minimal L2_timing_measurements table. rows: (channel, dt)."""
    db = tmp_path / "timestd.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE L2_timing_measurements ("
        " channel TEXT NOT NULL, timestamp_utc TEXT NOT NULL,"
        " minute_boundary_utc INTEGER, station TEXT, frequency_mhz REAL)"
    )
    con.executemany(
        "INSERT INTO L2_timing_measurements"
        " (channel, timestamp_utc, minute_boundary_utc, station, frequency_mhz)"
        " VALUES (?, ?, ?, ?, ?)",
        [(ch, dt.isoformat(), int(dt.timestamp()), "WWV", 10.0) for ch, dt in rows],
    )
    con.commit()
    con.close()
    return db


def _service(tmp_path: Path, db: Path) -> PhysicsFusionService:
    data_root = tmp_path / "data"
    (data_root / "phase2").mkdir(parents=True)
    (tmp_path / "out").mkdir(exist_ok=True)
    return PhysicsFusionService(
        data_root=data_root,
        output_dir=tmp_path / "out",
        storage_config={"sqlite_path": str(db)},
    )


def test_channels_come_from_the_store_not_the_file_tree(tmp_path):
    """No legacy directories exist at all — discovery must still work."""
    now = datetime.now(timezone.utc)
    db = _make_store(tmp_path, [("WWV_20000", now), ("SHARED_10000", now),
                                ("SHARED_10000", now - timedelta(minutes=1))])
    svc = _service(tmp_path, db)
    assert svc._discover_channels() == ["SHARED_10000", "WWV_20000"]


def test_empty_legacy_directories_do_not_invent_channels(tmp_path):
    """The mirror image: a leftover dir with no rows behind it is not a channel."""
    now = datetime.now(timezone.utc)
    db = _make_store(tmp_path, [("WWV_20000", now)])
    svc = _service(tmp_path, db)
    ghost = svc.data_root / "phase2" / "CHU_7850" / "clock_offset"
    ghost.mkdir(parents=True)
    assert svc._discover_channels() == ["WWV_20000"]


def test_freshness_reads_the_newest_row(tmp_path):
    now = datetime.now(timezone.utc)
    db = _make_store(tmp_path, [("WWV_20000", now - timedelta(seconds=30))])
    svc = _service(tmp_path, db)
    is_fresh, age = svc._check_upstream_freshness()
    assert is_fresh is True
    assert 0 <= age < 120, age


def test_freshness_reports_stale_when_the_store_stops_advancing(tmp_path):
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=3)
    db = _make_store(tmp_path, [("WWV_20000", old)])
    svc = _service(tmp_path, db)
    is_fresh, age = svc._check_upstream_freshness()
    assert is_fresh is False
    assert age > 3000, age


def test_freshness_does_not_depend_on_hdf5_files(tmp_path):
    """A fresh store with an empty legacy tree must NOT read as stale.

    This is the false alarm B4 has been emitting since the migration.
    """
    now = datetime.now(timezone.utc)
    db = _make_store(tmp_path, [("WWV_20000", now)])
    svc = _service(tmp_path, db)
    legacy = svc.data_root / "phase2" / "WWV_20000" / "clock_offset"
    legacy.mkdir(parents=True)          # exists, contains no .h5
    is_fresh, _ = svc._check_upstream_freshness()
    assert is_fresh is True


def test_missing_store_is_stale_not_a_crash(tmp_path):
    svc = _service(tmp_path, tmp_path / "absent.db")
    is_fresh, age = svc._check_upstream_freshness()
    assert is_fresh is False
    assert svc._discover_channels() == []
