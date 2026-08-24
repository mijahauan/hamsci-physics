"""The deploy manifest is a cross-repo contract; pin what must not drift.

Two things here are load-bearing beyond this repo:

* the GRAPE upload pipeline's ``source_id`` and transport ``name`` are the
  hs-uploader watermark keys for a KEEP-retention spool.  If they change,
  the daemon re-scans /var/lib/timestd/upload and re-ships every OBS*
  dataset already delivered to PSWS.
* ``grape-daily`` keeps its unit name across the split (operator runbooks),
  while the service units take the hamsci-physics- prefix.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def deploy():
    with open(ROOT / "deploy.toml", "rb") as fh:
        return tomllib.load(fh)


def test_package_identity(deploy):
    assert deploy["package"]["name"] == "hamsci-physics"


def test_grape_upload_watermark_keys_are_pinned(deploy):
    pipes = deploy["hs_uploader"]["pipeline"]
    grape = next(p for p in pipes if p["name"] == "grape-psws")
    # Byte-identical to hf-timestd's pre-split block.
    assert grape["source"]["source_id"] == "grape-datasets:/var/lib/timestd/upload"
    assert grape["transport"]["name"] == \
        "psws-grape-sftp:pswsnetwork.eng.ua.edu:{station_id}"
    assert grape["source"]["root"] == "/var/lib/timestd/upload"
    assert grape["source"]["retention"] == "keep"      # datasets stay local
    assert grape["source"]["match_dirs"] is True       # DRF dataset is a tree
    assert grape["transport"]["table"] == "grape.dataset"


def test_grape_daily_keeps_its_operator_facing_name(deploy):
    units = deploy["systemd"]["units"]
    assert "grape-daily.timer" in units
    assert not any(u.startswith("hamsci-physics-grape") for u in units)


def test_science_units_are_namespaced_and_shipped(deploy):
    shipped = {p.name for p in (ROOT / "systemd").glob("*")}
    for unit in deploy["systemd"]["units"]:
        assert unit in shipped, f"{unit} declared but not shipped"
    # The fusion stage is shipped but deliberately NOT enabled (see manifest).
    assert "hamsci-physics-fusion.service" in shipped
    assert "hamsci-physics-fusion.service" not in deploy["systemd"]["units"]


def test_no_unit_still_points_at_the_hf_timestd_venv():
    offenders = []
    for unit in (ROOT / "systemd").glob("*"):
        text = unit.read_text()
        if "/opt/git/sigmond/hf-timestd" in text or "hf_timestd" in text:
            offenders.append(unit.name)
    assert not offenders, offenders


def test_config_template_matches_the_frozen_data_root():
    with open(ROOT / "config" / "hamsci-physics-config.toml.template", "rb") as fh:
        cfg = tomllib.load(fh)
    assert cfg["paths"]["data_root"] == "/var/lib/timestd"
    assert set(cfg["station"]) >= {
        "callsign", "grid_square", "psws_station_id", "instrument_id"}
