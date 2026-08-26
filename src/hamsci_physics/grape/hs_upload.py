"""GRAPE → PSWS upload via the shared ``hs_uploader`` library.

This is the cutover from the bespoke ``SFTPUpload`` + ``queue.json``
path (``hamsci_physics.grape.uploader``) to ``hs_uploader``'s
``PswsDatasetSftp`` transport — the single PSWS wire-protocol code path
shared with mag-recorder and future instruments.

Each GRAPE daily dataset is an ``OBS<date>T00-00/`` Digital RF
*directory* tree.  ``PswsDatasetSftp`` uploads it recursively
(``mkdir``/``put`` walk) and then ``mkdir``'s the Grape trigger
directory so the PSWS server ingests it.  Retry + cursor state lives in
``/var/lib/hs-uploader/watermarks.db`` (the suite-wide watermark store),
replacing the per-instrument ``queue.json``.

Datasets are KEPT on disk after upload (matching
``[uploader.sftp].delete_after_upload = false``); the FileTreeSource
runs in ``keep`` retention and tracks shipped datasets by mtime cursor,
so they are not re-uploaded.

Why no post-upload verify: the original ``SFTPUpload.verify()`` re-``ls``'d
the trigger directory, but the PSWS server *consumes* that directory on
ingest — so the check produced false "Verification failed" negatives
even when the data landed.  A zero ``sftp`` return code is the success
signal here (same contract as the magnetometer path).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

from hs_uploader import Pipeline, StationIdentity, Uploader
from hs_uploader.sources import FileSpec, FileTreeSource
from hs_uploader.transports.psws_magnetometer import PswsDatasetSftp
from hs_uploader.watermark.sqlite import SqliteWatermarkStore, default_path

from . import psws_identity

logger = logging.getLogger(__name__)

GRAPE_TABLE = "grape.dataset"
PIPELINE_NAME = "grape-psws"
# One GRAPE invocation should drain every pending dataset (a backlog of
# missed days, or the single fresh dataset) rather than one per pump.
_PUMP_BUDGET = 64


def build_uploader(
    toml_config: Dict,
    upload_root: Path | str,
    *,
    dry_run: bool = False,
) -> Uploader:
    """Construct the GRAPE→PSWS ``Uploader`` from a parsed timestd config.

    Pulls identity/credentials from the same ``[station]`` /
    ``[uploader.sftp]`` blocks the legacy uploader used, so behaviour is
    unchanged apart from the transport implementation.
    """
    station = toml_config.get("station") or {}

    # Field names differ between this repo's config and the pre-split
    # hf-timestd one; psws_identity accepts both.  See that module.
    station_id = psws_identity.station_id(toml_config)
    instrument_id = psws_identity.instrument_id(toml_config)
    if not station_id:
        raise ValueError(
            "grape hs-upload: [station].psws_station_id (PSWS station id) "
            "is empty"
        )
    if not instrument_id:
        raise ValueError(
            "grape hs-upload: [station].instrument_id is empty"
        )

    ssh_key = psws_identity.ssh_key(toml_config)
    host = psws_identity.sftp_host(toml_config)
    bw = psws_identity.bandwidth_limit_kbps(toml_config)

    identity = StationIdentity(
        call=str(station.get("callsign", "")).strip(),
        grid=str(station.get("grid_square", "")).strip(),
        station_id=station_id,
        ssh_key_file=ssh_key or StationIdentity().ssh_key_file,
    )
    transport = PswsDatasetSftp(
        instrument_id=instrument_id,
        host=host,
        sftp_user=station_id,
        ssh_key_file=ssh_key or None,
        table=GRAPE_TABLE,
        bandwidth_limit_kbps=bw,
        dry_run=dry_run,
        name=f"psws-grape-sftp:{host}:{station_id}",
    )
    source = FileTreeSource(
        root=Path(upload_root),
        specs=[FileSpec(pattern="OBS*", parser=None, table=GRAPE_TABLE)],
        retention=FileTreeSource.KEEP,
        match_dirs=True,
        source_id=f"grape-datasets:{Path(upload_root)}",
    )
    pipe = Pipeline(
        name=PIPELINE_NAME,
        source=source,
        transport=transport,
        watermark=SqliteWatermarkStore(default_path()),
        identity=identity,
        max_records_per_pump=_PUMP_BUDGET,
    )
    return Uploader([pipe])


def run_upload(
    toml_config: Dict,
    upload_root: Path | str,
    *,
    dry_run: bool = False,
) -> int:
    """Build and drain the GRAPE→PSWS pipeline; return pump passes done."""
    up = build_uploader(toml_config, upload_root, dry_run=dry_run)
    passes = up.pump_until_idle()
    logger.info(
        "grape hs-upload: drained pipeline (%d pump pass%s)",
        passes, "" if passes == 1 else "es",
    )
    return passes
