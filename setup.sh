#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 not found"; }

echo "== Codex Jarvis setup =="
need python3
need systemctl
need codex

codex login status >/dev/null 2>&1 || die "Codex is not logged in; run: codex login"
echo "Codex account: authenticated"

INSTALL_USER="$(id -un)"
INSTALL_DIR="$ROOT"
DEFAULT_CWD="${CODEX_JARVIS_CWD:-$HOME}"
read -rp "Codex workspace [$DEFAULT_CWD]: " CODEX_CWD
CODEX_CWD="${CODEX_CWD:-$DEFAULT_CWD}"
[[ -d "$CODEX_CWD" ]] || die "workspace does not exist: $CODEX_CWD"
CODEX_CWD="$(cd "$CODEX_CWD" && pwd)"

DEFAULT_HOME="$ROOT/codex_home"
read -rp "Dedicated CODEX_HOME [$DEFAULT_HOME]: " CODEX_HOME_DIR
CODEX_HOME_DIR="${CODEX_HOME_DIR:-$DEFAULT_HOME}"
CODEX_HOME_DIR="$(mkdir -p "$CODEX_HOME_DIR" && cd "$CODEX_HOME_DIR" && pwd)"

MCP_VENV="${CODEX_JARVIS_MCP_VENV:-$ROOT/.venv}"
echo "Installing MCP dependencies into $MCP_VENV"
python3 -m venv "$MCP_VENV"
"$MCP_VENV/bin/python" -m pip install --upgrade pip >/dev/null
"$MCP_VENV/bin/python" -m pip install -r requirements-mcp.txt

AUTH_SOURCE="${CODEX_JARVIS_AUTH_FILE:-$HOME/.codex/auth.json}"
[[ -f "$AUTH_SOURCE" ]] || die "Codex auth file not found: $AUTH_SOURCE"
if [[ ! -e "$CODEX_HOME_DIR/auth.json" ]]; then
    ln -s "$AUTH_SOURCE" "$CODEX_HOME_DIR/auth.json"
fi

sed \
    -e "s|__MCP_PYTHON__|$MCP_VENV/bin/python|g" \
    -e "s|__INSTALL_DIR__|$INSTALL_DIR|g" \
    codex-config.toml.example > "$CODEX_HOME_DIR/config.toml"
chmod 700 "$CODEX_HOME_DIR"

read -rp "systemd service name [codex-jarvis]: " SERVICE_NAME
SERVICE_NAME="${SERVICE_NAME:-codex-jarvis}"
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@-]+$ ]] || die "invalid service name"

UNIT_FILE="$(mktemp "/tmp/${SERVICE_NAME}.service.XXXXXX")"
trap 'rm -f "$UNIT_FILE"' EXIT
sed \
    -e "s|__USER__|$INSTALL_USER|g" \
    -e "s|__INSTALL_DIR__|$INSTALL_DIR|g" \
    -e "s|__CODEX_HOME__|$CODEX_HOME_DIR|g" \
    -e "s|__CODEX_CWD__|$CODEX_CWD|g" \
    codex-jarvis.service.example > "$UNIT_FILE"

python3 -m py_compile app_server.py codex_ask_watcher.py
echo "Installing /etc/systemd/system/${SERVICE_NAME}.service (sudo required)"
sudo install -m 0644 "$UNIT_FILE" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.service"

echo
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
echo
echo "Installed. The CodexAsk module still needs to be uploaded to the Telethon userbot via .lm."
