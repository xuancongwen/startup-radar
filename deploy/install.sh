#!/usr/bin/env bash
# Install Startup Radar as three systemd services on a plain Linux host or an LXC container.
# No Docker. Idempotent: rerun after a git pull to upgrade the code in place.
#
#   sudo deploy/install.sh
#
# Environment overrides: CERTSTREAM_VERSION (default 1.10.1), RADAR_HOME (/opt/startup-radar),
# RADAR_STATE (/var/lib/startup-radar), RADAR_ETC (/etc/startup-radar).
set -euo pipefail

CERTSTREAM_VERSION="${CERTSTREAM_VERSION:-1.10.1}"
RADAR_HOME="${RADAR_HOME:-/opt/startup-radar}"
RADAR_STATE="${RADAR_STATE:-/var/lib/startup-radar}"
RADAR_ETC="${RADAR_ETC:-/etc/startup-radar}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)"; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required (3.11 or newer)"; exit 1; }
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' \
  || { echo "python3 must be 3.11 or newer; found $(python3 --version)"; exit 1; }
python3 -c 'import venv, ensurepip' 2>/dev/null \
  || { echo "python3 venv support is missing; on Debian/Ubuntu: apt install python3-venv"; exit 1; }
command -v curl >/dev/null || { echo "curl is required"; exit 1; }

case "$(uname -m)" in
  x86_64)  ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  armv7l)  ARCH=arm ;;
  i?86)    ARCH=i386 ;;
  *) echo "unsupported architecture $(uname -m)"; exit 1 ;;
esac

echo "== user and directories"
id radar >/dev/null 2>&1 || useradd --system --home-dir "$RADAR_STATE" --shell /usr/sbin/nologin radar
install -d -o radar -g radar -m 750 "$RADAR_STATE" "$RADAR_STATE/shortlists"
install -d -o root -g root -m 755 "$RADAR_HOME" "$RADAR_ETC"

echo "== certstream-server-go $CERTSTREAM_VERSION ($ARCH)"
BIN=/usr/local/bin/certstream-server-go
if ! [ -x "$BIN" ] || ! "$BIN" --version 2>/dev/null | grep -q "$CERTSTREAM_VERSION"; then
  TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
  BASE="https://github.com/d-Rickyy-b/certstream-server-go/releases/download/v${CERTSTREAM_VERSION}"
  NAME="certstream-server-go_${CERTSTREAM_VERSION}_linux_${ARCH}"
  curl -fsSL -o "$TMP/$NAME" "$BASE/$NAME"
  curl -fsSL -o "$TMP/sums" "$BASE/certstream-server-go_${CERTSTREAM_VERSION}_checksums.txt"
  (cd "$TMP" && grep " $NAME\$" sums | sha256sum -c - >/dev/null) || { echo "checksum mismatch for $NAME"; exit 1; }
  install -m 755 "$TMP/$NAME" "$BIN"
fi
install -m 644 "$SRC/deploy/certstream.systemd.yaml" "$RADAR_ETC/certstream.yaml"
"$BIN" validate -c "$RADAR_ETC/certstream.yaml" >/dev/null

echo "== application code and virtualenv"
install -m 644 "$SRC/pyproject.toml" "$RADAR_HOME/pyproject.toml"
rm -rf "$RADAR_HOME/startup_radar"
cp -r "$SRC/startup_radar" "$RADAR_HOME/startup_radar"
find "$RADAR_HOME/startup_radar" -name __pycache__ -type d -prune -exec rm -rf {} +
[ -x "$RADAR_HOME/.venv/bin/python" ] || python3 -m venv "$RADAR_HOME/.venv"
"$RADAR_HOME/.venv/bin/pip" install --quiet --upgrade pip
"$RADAR_HOME/.venv/bin/pip" install --quiet "$RADAR_HOME"

echo "== configuration"
if [ ! -f "$RADAR_ETC/env" ]; then
  SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  sed "s/change-me-to-a-long-random-secret/$SECRET/" "$SRC/deploy/env.example" > "$RADAR_ETC/env"
  chmod 600 "$RADAR_ETC/env"
  echo "   wrote $RADAR_ETC/env with a generated token for client 'mondayflow-acme'"
else
  echo "   keeping existing $RADAR_ETC/env"
fi

echo "== systemd units"
for unit in certstream startup-radar startup-radar-api; do
  install -m 644 "$SRC/deploy/systemd/$unit.service" "/etc/systemd/system/$unit.service"
done
systemctl daemon-reload
systemctl enable --now certstream startup-radar startup-radar-api
systemctl restart startup-radar startup-radar-api

echo
echo "Installed. Check:  systemctl status certstream startup-radar startup-radar-api"
echo "Tokens and limits: $RADAR_ETC/env  (then: systemctl restart startup-radar-api)"
echo "Data:              $RADAR_STATE  (radar.sqlite3, shortlists/)"
echo "Try:               curl -H \"Authorization: Bearer \$(sed -n 's/^RADAR_TOKENS=[^:]*://p' $RADAR_ETC/env)\" http://127.0.0.1:8081/stats"
