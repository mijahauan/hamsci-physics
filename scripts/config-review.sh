#!/usr/bin/env bash
# `smd config edit hamsci-physics` — show the config and self-validate.
set -euo pipefail

CONFIG=${CONFIG:-/etc/hamsci-physics/config.toml}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV_PY=${VENV_PY:-/opt/git/sigmond/hamsci-physics/venv/bin/python3}
[[ -x $VENV_PY ]] || VENV_PY=python3

if [[ ! -f $CONFIG ]]; then
    echo "hamsci-physics: $CONFIG not found — run 'smd config init hamsci-physics'." >&2
    exit 1
fi

echo "=== $CONFIG ==="
cat "$CONFIG"
echo
echo "=== validate ==="
"$VENV_PY" -m hamsci_physics.cli validate --config "$CONFIG"
