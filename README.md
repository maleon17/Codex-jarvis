# codex-ask

> Part of **[telegram-ai](https://github.com/maleon17/telegram-ai)** — Claude/Codex ↔ Telegram, four ways.

A standalone product for invoking Codex through a Telethon/Hikka userbot.
This is not `codex-telegram-bridge` — the standalone Bot API application lives
in a different repository and isn't imported by this project.

Repository: <https://github.com/maleon17/codex-ask>

## What's included

- `codex_ask.py` — the loadable `CodexAsk` module for the userbot;
- `codex_ask_watcher.py` — the persistent Codex app-server worker;
- `telegram_actions_mcp.py` — MCP tools for real Telegram actions;
- `app_server.py` — a minimal JSONL client for the Codex app-server;
- `setup.sh` and `codex-jarvis.service.example` — worker installation.

The module's commands deliberately start with `x` so they don't collide
with ClaudeAsk: `.xask`, `.xsearch`, `.xtranslate`, `.xnew`. Codex history
is stored in its own `CodexAsk` namespace and its own `instance_id`. The
trigger-rule storage is shared with ClaudeAsk (`ClaudeAsk`), and both
modules each have their own active `@loader.watcher()` on incoming
messages — both call the shared `JarvisAsk` coordinator (`handle_message`),
which filters by each trigger's `engine` field (`claude`/`codex`), so a
given trigger is handled by exactly one engine and never fires twice. When
its own backend is unavailable (account limit, timeout),
`JarvisAsk.fallback()` switches both the trigger's agent actions and its
verify classification to the other engine.

## Architecture

`CodexAsk` sends requests to `/xask`, and the worker returns progress and
the answer via `/tmp/jarvisask_xask_*`. For account actions, the MCP layer
queues a call in `/tmp/jarvisask_tool_queue`; the loaded userbot module
executes it with its own Telethon session and returns the result. The
`cmd_queue.py` queue is shared transport for ClaudeAsk and CodexAsk, and is
not part of the standalone Bot API application.

## Worker installation

Requirements: Linux, Python 3.10+, systemd, the Codex CLI installed and
authenticated (`codex login`). The host must also be running the shared
queue relay with the `/xask`, `/xreset`, and Telegram tool endpoints. The
userbot itself must be installed separately — this project targets the
[Heroku](https://github.com/coddrago/Heroku) userbot; follow its own README
for installing and starting the userbot before loading `codex_ask.py` into
it. Heroku can run on a dedicated Telethon/Hikka host or in Docker on the
same machine as this backend — running it on a separate host is just this
deployment's own choice, not a requirement.

```bash
git clone https://github.com/maleon17/codex-ask.git
cd codex-ask
chmod +x setup.sh
./setup.sh
```

The script creates a dedicated `CODEX_HOME`, a virtual environment for MCP,
and a `config.toml` with local paths. Authentication is picked up from
`$HOME/.codex/auth.json` via a symlink and is never copied into Git.
Runtime files (`codex_home/`, `state/`, `.venv/`) are gitignored.

For a manual install, you can copy `codex-jarvis.service.example`, replace
`__USER__`, `__INSTALL_DIR__`, `__CODEX_HOME__`, and `__CODEX_CWD__`, then
run `sudo systemctl daemon-reload && sudo systemctl enable --now codex-jarvis.service`.

## Loading the userbot module

Send `codex_ask.py` as a document to the dedicated test Telegram channel and
reply to the document with `.lm`. Once loaded, `.xask` and the rest of the
commands become available in the userbot. Userbot/Telegram tokens are not
part of this repository.

After loading, run `.xasknet local <instance_id>` or
`.xasknet tailnet <instance_id> <backend_url>` once to configure the
network for that instance. This setting replaces configuration via
environment variables only.

## Updating

To reinstall the loaded module from the latest version on the `main`
branch, run:

```text
.dlm https://raw.githubusercontent.com/maleon17/codex-ask/main/codex_ask.py
```

Each instance's `.xasknet` settings are stored in Heroku's persistent
database and are not removed by such an update, so no reconfiguration is
needed.

## Verification

```bash
python3 -m py_compile app_server.py codex_ask_watcher.py
systemctl status codex-jarvis
journalctl -u codex-jarvis -f
```

After loading the module, verify a plain `.xask` and a request that
requires a real tool (`list_triggers`, `read_history`, or `search_chat`).
The final answer is only produced after the tool's actual result, never
from the model's own unverified self-report.

## Product boundaries

`codex-telegram-bot` is a separate Telegram Bot API interface for Codex.
`Claude-jarvis` is the separate ClaudeAsk userbot/backend. This repository
is only the CodexAsk userbot/backend; only the queue relay and the trigger
namespace are shared, because that's infrastructure transport and a
deliberately shared rule store.

## License

MIT, see `LICENSE`.
