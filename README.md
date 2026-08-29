# Codex Jarvis

Это отдельный продукт для Telethon/Heroku userbot. Он не является частью
`codex-telegram-bot` и не использует Telegram Bot API-токен.

## Состав

- `codex_ask.py` - загружаемый userbot-модуль `CodexAsk`;
- `codex_ask_watcher.py` - постоянный Codex app-server worker;
- `telegram_actions_mcp.py` - MCP-адаптер реальных действий Telegram;
- `app_server.py` - минимальный JSONL-клиент app-server.

## Команды модуля

Команды специально имеют префикс `x`, чтобы не пересекаться с ClaudeAsk:

- `.xask <вопрос>`;
- `.xsearch <слово>`;
- `.xtranslate <текст>`;
- `.xnew`.

История диалога хранится отдельно в namespace `CodexAsk`. Правила триггеров
остаются в namespace `ClaudeAsk`: это существующее хранилище userbot, поэтому
старые триггеры не мигрируются и не дублируются. ClaudeAsk остаётся единственным
watcher входящих сообщений; CodexAsk обслуживает собственную очередь и MCP
tool calls для `andrey_codex`.

## Очередь и MCP

`cmd_queue.py` - общий транспорт, не часть ни одного интерфейса. Модуль пишет
в `/xask`, worker читает `/tmp/hermes_xask_queue`, а результаты кладёт в
`/tmp/hermes_xask_result`. Для Telegram actions MCP использует
`/tmp/hermes_tool_queue`; активный userbot возвращает результаты через
`/tool_call_result`.

Перед запуском worker нужен отдельный `CODEX_HOME` с авторизацией Codex и
конфигурацией `codex-config.toml`. На этой машине это можно подготовить так:

```sh
mkdir -p /home/mishin/codex-jarvis/codex_home
ln -s /home/mishin/.codex/auth.json /home/mishin/codex-jarvis/codex_home/auth.json
cp /home/mishin/codex-jarvis/codex-config.toml /home/mishin/codex-jarvis/codex_home/config.toml
chmod 700 /home/mishin/codex-jarvis/codex_home
```

Установка systemd: заменить `__USER__` в `codex-jarvis.service.example`, затем
положить unit в `/etc/systemd/system/` и выполнить `systemctl enable --now`.

## Загрузка модуля

Загрузка в userbot выполняется через существующий тестботовый канал из
`BRIDGE_PROJECT_HANDOFF.md`: отправить `codex_ask.py` документом и ответить
командой `.lm`. Секреты и токены в репозитории не хранятся.
