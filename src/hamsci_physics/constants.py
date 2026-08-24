"""Physical and station constants for the hamsci-physics products.

Values are carried over byte-identical from hf-timestd's ``wwv_constants``
(split Phase 3, 2026-08-24) so that migrating a station changes *where* the
science runs, never *what it computes*.  Two deliberate non-changes:

* ``EARTH_RADIUS_KM`` stays 6371.0 — hamsci-dsp's ``R_EARTH_KM`` is the more
  precise 6371.0088, and adopting it here would silently shift every
  reanalysis result.  Any switch must be its own, measured commit.
* the per-station frequency lists are derived from the hamsci-dsp catalog,
  exactly as hf-timestd derives them, so they cannot drift from the shared
  station data.

Timing schedules, tone frequencies and detection thresholds stay in
hf-timestd; only what the physics products actually read lives here.
"""
from __future__ import annotations

from hamsci_dsp.stations import BUILTIN_CATALOG as STATION_CATALOG

# --- station broadcast frequencies (MHz), from the shared catalog ----------
WWV_FREQUENCIES = list(STATION_CATALOG.get('WWV').frequencies_mhz)
WWVH_FREQUENCIES = list(STATION_CATALOG.get('WWVH').frequencies_mhz)  # NOT 20/25 MHz
CHU_FREQUENCIES = list(STATION_CATALOG.get('CHU').frequencies_mhz)
BPM_FREQUENCIES = list(STATION_CATALOG.get('BPM').frequencies_mhz)

# --- station coordinates (degrees), from the shared catalog ----------------
WWV_LAT = STATION_CATALOG.get('WWV').lat
WWV_LON = STATION_CATALOG.get('WWV').lon
WWVH_LAT = STATION_CATALOG.get('WWVH').lat
WWVH_LON = STATION_CATALOG.get('WWVH').lon
CHU_LAT = STATION_CATALOG.get('CHU').lat
CHU_LON = STATION_CATALOG.get('CHU').lon
BPM_LAT = STATION_CATALOG.get('BPM').lat
BPM_LON = STATION_CATALOG.get('BPM').lon

# --- physics ---------------------------------------------------------------
SPEED_OF_LIGHT_KM_S = 299792.458
EARTH_RADIUS_KM = 6371.0     # see module docstring before "improving" this
E_LAYER_HEIGHT_KM = 110.0
F_LAYER_HEIGHT_KM = 300.0

# Anchor-quality threshold the reanalysis uses to pick trustworthy arrivals.
ANCHOR_SNR_HIGH = 15.0       # Very confident anchor
