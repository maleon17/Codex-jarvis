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

# --- Jarvis persona: profanity notice + env (owner + softening list) ------
cat <<'NOTE'

Note: the Jarvis persona ships with profanity and dark humour enabled by
default - a deliberately informal, entertainment-leaning bot. To tone that
down, edit the persona after setup with the  .persona  command in Telegram
(CodexAsk: .xpersona), or edit personas/<instance>.md directly (re-read
automatically on change, no restart).
NOTE

_jarvis_clean() {  # drop control chars + shell/systemd-hazardous chars, keep UTF-8 letters
    python3 - "$1" <<'PY'
import sys
bad = set('\\"\'`$')
s = sys.argv[1] if len(sys.argv) > 1 else ""
sys.stdout.write("".join(c for c in s if c >= " " and c != "\x7f" and c not in bad))
PY
}

JARVIS_ENV="$ROOT/jarvis.env"
if [ -t 0 ]; then
    read -rp "Owner display name for the persona template (blank to skip): " J_OWNER_NAME || J_OWNER_NAME=""
    read -rp "Owner Telegram numeric id for the persona template (blank to skip): " J_OWNER_TG_ID || J_OWNER_TG_ID=""
    read -rp "Chat ids where profanity is suppressed, comma-separated (blank for none): " J_NOMATS || J_NOMATS=""
    read -rp "Shared persona directory [$ROOT/personas]: " J_PERSONA_DIR || J_PERSONA_DIR=""
    J_OWNER_NAME="$(_jarvis_clean "$J_OWNER_NAME")"
    J_OWNER_TG_ID="$(printf '%s' "$J_OWNER_TG_ID" | tr -cd '0-9')"
    J_NOMATS="$(printf '%s' "$J_NOMATS" | tr -cd '0-9,-')"
    J_PERSONA_DIR="$(_jarvis_clean "${J_PERSONA_DIR:-$ROOT/personas}")"
    if [ "$J_PERSONA_DIR" != "$ROOT/personas" ]; then
        mkdir -p "$J_PERSONA_DIR"
        [ -f "$J_PERSONA_DIR/default.md.example" ] || \
            cp "$ROOT/personas/default.md.example" "$J_PERSONA_DIR/default.md.example"
    fi
    ( umask 177; : > "$JARVIS_ENV" )
    {
        [ -n "$J_OWNER_NAME" ]  && printf 'JARVIS_OWNER_NAME=%s\n'  "$J_OWNER_NAME"
        [ -n "$J_OWNER_TG_ID" ] && printf 'JARVIS_OWNER_TG_ID=%s\n' "$J_OWNER_TG_ID"
        printf 'JARVIS_NOMATS_CHAT_IDS=%s\n' "$J_NOMATS"
        printf 'JARVIS_PERSONA_DIR=%s\n' "$J_PERSONA_DIR"
    } >> "$JARVIS_ENV"
    chmod 600 "$JARVIS_ENV"
    echo "Wrote $JARVIS_ENV (git-ignored; the watcher loads it via EnvironmentFile=)."
    echo "The shared cmd_queue.py (from Claude-jarvis) serves /xpersona from its own"
    echo "\$JARVIS_XPERSONA_DIR (defaults to \$JARVIS_PERSONA_DIR). Set"
    echo "JARVIS_XPERSONA_DIR=$J_PERSONA_DIR in that relay's service unit, otherwise"
    echo ".xpersona writes will not reach this watcher."
fi

python3 -m py_compile app_server.py codex_ask_watcher.py
echo "Installing /etc/systemd/system/${SERVICE_NAME}.service (sudo required)"
sudo install -m 0644 "$UNIT_FILE" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.service"

echo
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
echo
echo "Installed. The CodexAsk module still needs to be uploaded to the Telethon userbot via .lm."

# --- optional: graphify code-map -------------------------------------------
# graphify (PyPI package "graphifyy", github.com/Graphify-Labs/graphify) turns
# this repo into a queryable knowledge graph under graphify-out/. Optional.
setup_graphify() {
    local platform="$1"
    if command -v graphify >/dev/null 2>&1; then
        echo "graphify: already on PATH ($(command -v graphify))"
    elif command -v uv >/dev/null 2>&1; then
        uv tool install graphifyy || { echo "graphify: install failed, skipping"; return 0; }
    elif command -v pipx >/dev/null 2>&1; then
        pipx install graphifyy || { echo "graphify: install failed, skipping"; return 0; }
    else
        echo "graphify: needs 'uv' or 'pipx' to install - skipping"
        return 0
    fi
    graphify install --platform "$platform" >/dev/null 2>&1 || true
    graphify update . >/dev/null 2>&1 || true
    echo "graphify: code map built under graphify-out/ (re-run 'graphify update .' after edits;"
    echo "          a post-commit hook keeps it fresh if graphify installed one)"
}

if [ -t 0 ]; then
    read -rp "Set up the graphify code-map for this repo? [y/N] " _SETUP_GRAPHIFY || _SETUP_GRAPHIFY=""
    case "${_SETUP_GRAPHIFY:-}" in
        [Yy]*) setup_graphify "codex" ;;
    esac
fi
