#!/usr/bin/env bash
# `smd config init hamsci-physics` — render /etc/hamsci-physics/config.toml.
#
# CONTRACT §14.3: sigmond exports the station identity as STATION_* env vars,
# so on a host that already configured hf-timestd the operator answers
# nothing twice.  Any value already present in an existing config wins over
# the template default; the env bag wins over both.
set -euo pipefail

CONFIG_DIR=${CONFIG_DIR:-/etc/hamsci-physics}
CONFIG=${CONFIG:-$CONFIG_DIR/config.toml}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TEMPLATE=$REPO/config/hamsci-physics-config.toml.template

install -d -m 0755 "$CONFIG_DIR"

if [[ -f $CONFIG ]]; then
    echo "hamsci-physics: $CONFIG exists — leaving it alone (use 'smd config edit')."
    exit 0
fi

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
cp "$TEMPLATE" "$tmp"

set_key() {  # set_key <key> <value>
    local key=$1 val=$2
    [[ -z $val ]] && return 0
    sed -i "s|^\( *${key} *= *\)\"\"|\1\"${val}\"|" "$tmp"
}

set_key callsign        "${STATION_CALLSIGN:-}"
set_key grid_square     "${STATION_GRID:-${STATION_GRID_SQUARE:-}}"
set_key psws_station_id "${STATION_PSWS_STATION_ID:-}"
set_key instrument_id   "${STATION_PSWS_INSTRUMENT_ID:-}"

install -m 0644 "$tmp" "$CONFIG"
echo "hamsci-physics: wrote $CONFIG"
echo "  station: ${STATION_CALLSIGN:-<unset>} / ${STATION_GRID:-<unset>}"
[[ -z ${STATION_PSWS_STATION_ID:-} ]] && \
    echo "  note: no PSWS station id — science runs locally, GRAPE upload is skipped."
exit 0
