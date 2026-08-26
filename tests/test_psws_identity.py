"""GRAPE→PSWS identity resolution across both config shapes.

Regression cover for the 2026-08-25 outage: ``/etc/hamsci-physics/config.toml``
carried ``[station].psws_station_id = "S000170"``, contract.py validated that
name and reported the client green, but the uploader read ``[station].id``,
found "", and refused to ship.  Two nights of GRAPE data went nowhere while
every status surface said OK.

``psws_identity`` is loaded straight off disk rather than through
``hamsci_physics.grape`` so these assertions run without numpy/h5py/hs_uploader
-- the resolution logic is pure stdlib and deserves to be testable as such.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_MOD = (Path(__file__).resolve().parents[1]
        / "src" / "hamsci_physics" / "grape" / "psws_identity.py")
_spec = importlib.util.spec_from_file_location("psws_identity", _MOD)
psws_identity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(psws_identity)


# The shape this repo installs (mirrors mag-recorder's field names).
SPLIT_CONFIG = {
    "station": {
        "callsign": "AC0G",
        "grid_square": "EM38ww",
        "psws_station_id": "S000170",
        "instrument_id": "171",
    },
}

# The shape GRAPE read before the split, still present on un-migrated hosts.
LEGACY_CONFIG = {
    "station": {
        "callsign": "AC0G",
        "grid_square": "EM38ww",
        "id": "S000170",
        "instrument_id": "171",
    },
    "uploader": {
        "sftp": {
            "host": "pswsnetwork.eng.ua.edu",
            "ssh_key": "/home/timestd/.ssh/id_rsa_psws",
            "bandwidth_limit_kbps": 0,
        },
    },
}


class TestStationId:
    def test_reads_the_split_config_field_name(self):
        assert psws_identity.station_id(SPLIT_CONFIG) == "S000170"

    def test_still_reads_the_legacy_field_name(self):
        assert psws_identity.station_id(LEGACY_CONFIG) == "S000170"

    def test_new_name_wins_when_both_are_present(self):
        cfg = {"station": {"psws_station_id": "S000170", "id": "S000999"}}
        assert psws_identity.station_id(cfg) == "S000170"

    def test_legacy_name_used_when_new_one_is_blank(self):
        cfg = {"station": {"psws_station_id": "  ", "id": "S000170"}}
        assert psws_identity.station_id(cfg) == "S000170"

    @pytest.mark.parametrize("cfg", [
        {}, {"station": {}}, {"station": {"psws_station_id": ""}},
        {"station": None},
    ])
    def test_empty_when_unset(self, cfg):
        assert psws_identity.station_id(cfg) == ""

    def test_instrument_id_is_spelled_the_same_in_both(self):
        assert psws_identity.instrument_id(SPLIT_CONFIG) == "171"
        assert psws_identity.instrument_id(LEGACY_CONFIG) == "171"


class TestSshKey:
    def test_legacy_uploader_sftp_ssh_key(self):
        assert (psws_identity.ssh_key(LEGACY_CONFIG)
                == "/home/timestd/.ssh/id_rsa_psws")

    def test_split_uploader_ssh_key_file(self):
        cfg = {"uploader": {"ssh_key_file": "/home/timestd/.ssh/id_rsa_psws"}}
        assert (psws_identity.ssh_key(cfg)
                == "/home/timestd/.ssh/id_rsa_psws")

    def test_falls_back_to_the_timestd_readable_key(self):
        """NOT hs_uploader's default, which is 0600 root:hsupload."""
        assert (psws_identity.ssh_key(SPLIT_CONFIG)
                == psws_identity.DEFAULT_SSH_KEY)
        assert psws_identity.DEFAULT_SSH_KEY.startswith("/home/timestd/")

    def test_expands_tilde(self, monkeypatch):
        monkeypatch.setenv("HOME", "/home/timestd")
        cfg = {"uploader": {"ssh_key_file": "~/.ssh/id_rsa_psws"}}
        assert (psws_identity.ssh_key(cfg)
                == "/home/timestd/.ssh/id_rsa_psws")


class TestHostAndBandwidth:
    def test_host_default(self):
        assert psws_identity.sftp_host({}) == "pswsnetwork.eng.ua.edu"

    def test_host_override_either_shape(self):
        assert psws_identity.sftp_host(
            {"uploader": {"host": "h1"}}) == "h1"
        assert psws_identity.sftp_host(
            {"uploader": {"sftp": {"host": "h2"}}}) == "h2"

    def test_zero_bandwidth_means_uncapped(self):
        assert psws_identity.bandwidth_limit_kbps(LEGACY_CONFIG) is None
        assert psws_identity.bandwidth_limit_kbps({}) is None

    def test_bandwidth_cap_is_read(self):
        assert psws_identity.bandwidth_limit_kbps(
            {"uploader": {"bandwidth_limit_kbps": 100}}) == 100

    def test_garbage_bandwidth_is_ignored_not_fatal(self):
        assert psws_identity.bandwidth_limit_kbps(
            {"uploader": {"bandwidth_limit_kbps": "fast"}}) is None


def test_the_config_that_actually_broke_production_now_resolves():
    """Verbatim [station] block from B4's /etc/hamsci-physics/config.toml."""
    assert psws_identity.station_id(SPLIT_CONFIG) == "S000170"
    assert psws_identity.instrument_id(SPLIT_CONFIG) == "171"
    assert psws_identity.ssh_key(SPLIT_CONFIG)
