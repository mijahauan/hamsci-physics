"""Resolve the PSWS upload identity from a parsed hamsci-physics config.

Two config shapes are in the wild and both have to work:

* ``/etc/hamsci-physics/config.toml`` -- this repo's own file, created by
  the 2026-08-24 split.  It deliberately mirrors *mag-recorder's* field
  names: ``[station].psws_station_id`` and ``[uploader].ssh_key_file``.
* ``/etc/hf-timestd/timestd-config.toml`` -- what GRAPE read before the
  split, and what an un-migrated host still has.  It uses the older
  ``[station].id`` and ``[uploader.sftp].ssh_key``.

Reading only the *second* shape is what silently killed GRAPE uploads on
2026-08-25: the installed config had ``psws_station_id`` set, contract.py
validated that name and reported the client green, while the uploader read
``id``, found ``""``, and raised "[station].id (PSWS station id) is empty"
every night.  One config, two names, two readers that disagreed.

So: accept both spellings, prefer the new one, and keep this module free of
third-party imports so the resolution can be unit-tested without numpy,
h5py or hs_uploader present.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

# grape-daily.service pins ``User=timestd``, and this is the key registered
# with PSWS for that user.  hs_uploader's own default
# (/etc/hs-uploader/keys/id_ed25519) is mode 0600 root:hsupload -- correct
# for the daemon, unreadable by timestd -- so it is the wrong fallback for
# the in-process path and would fail auth rather than fail loudly.
DEFAULT_SSH_KEY = "/home/timestd/.ssh/id_rsa_psws"

DEFAULT_HOST = "pswsnetwork.eng.ua.edu"


def _section(config: Dict, *path: str) -> Dict:
    """Walk a dotted TOML path, returning {} for any missing/!dict level."""
    cur: Any = config
    for key in path:
        if not isinstance(cur, dict):
            return {}
        cur = cur.get(key) or {}
    return cur if isinstance(cur, dict) else {}


def _first(*values: Any) -> str:
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def station_id(config: Dict) -> str:
    """PSWS station id, e.g. ``S000170``.  New name wins; ``""`` if unset."""
    station = _section(config, "station")
    return _first(station.get("psws_station_id"), station.get("id"))


def instrument_id(config: Dict) -> str:
    """PSWS instrument id, e.g. ``171``.  Spelled the same in both shapes."""
    return _first(_section(config, "station").get("instrument_id"))


def ssh_key(config: Dict, *, default: Optional[str] = DEFAULT_SSH_KEY) -> str:
    """Private key for the PSWS SFTP login, ``~`` expanded."""
    key = _first(
        _section(config, "uploader").get("ssh_key_file"),
        _section(config, "uploader", "sftp").get("ssh_key"),
        default,
    )
    return os.path.expanduser(key) if key else ""


def sftp_host(config: Dict) -> str:
    """PSWS SFTP host; both shapes may override it, else the default."""
    return _first(
        _section(config, "uploader").get("host"),
        _section(config, "uploader", "sftp").get("host"),
        DEFAULT_HOST,
    )


def bandwidth_limit_kbps(config: Dict) -> Optional[int]:
    """Transfer cap in kbit/s.  ``0``/empty means "no cap" (None)."""
    for block in (_section(config, "uploader"),
                  _section(config, "uploader", "sftp")):
        bw = block.get("bandwidth_limit_kbps")
        if bw in (0, "0", "", None):
            continue
        try:
            return int(bw)
        except (TypeError, ValueError):
            continue
    return None
