# Codex Jarvis

Отдельный продукт для вызова Codex через Telethon/Hikka userbot. Это не
`Codex-telegram-bot`: standalone Bot API-приложение живёт в другом репозитории
и не импортируется этим проектом.

Репозиторий: <https://github.com/maleon17/Codex-jarvis>

## Что входит

- `codex_ask.py` — загружаемый модуль `CodexAsk` для userbot;
- `codex_ask_watcher.py` — постоянный Codex app-server worker;
- `telegram_actions_mcp.py` — MCP-инструменты реальных действий Telegram;
- `app_server.py` — минимальный JSONL-клиент Codex app-server;
- `setup.sh` и `codex-jarvis.service.example` — установка worker-а.

Команды модуля намеренно начинаются с `x`, чтобы не пересекаться с
ClaudeAsk: `.xask`, `.xsearch`, `.xtranslate`, `.xnew`. История Codex хранится
в собственном namespace `CodexAsk` и собственном `instance_id`. Хранилище
правил триггеров общее с ClaudeAsk (`ClaudeAsk`), и у обоих модулей есть свой
активный `@loader.watcher()` на входящие сообщения — оба вызывают общий
координатор `JarvisAsk` (`handle_message`), который фильтрует по полю
`engine` каждого триггера (`claude`/`codex`), поэтому конкретный триггер
обрабатывается ровно одним движком и не срабатывает дважды. При недоступности
своего бэкенда (лимит аккаунта, таймаут) `JarvisAsk.fallback()` переключает
и агентские действия триггера, и verify-классификацию на второй движок.

## Архитектура

`CodexAsk` отправляет запросы в `/xask`, а worker возвращает прогресс и ответ
через `/tmp/hermes_xask_*`. Для действий аккаунта MCP ставит вызов в
`/tmp/hermes_tool_queue`; загруженный userbot-модуль выполняет его своей
Telethon-сессией и возвращает результат. Очередь `cmd_queue.py` — общий
транспорт для ClaudeAsk и CodexAsk, не часть standalone Bot API-приложения.

## Установка worker-а

Требования: Linux, Python 3.10+, systemd, установленный Codex CLI и активная
авторизация (`codex login`). На хосте также должен работать общий queue relay с
маршрутом `/xask`, `/xreset` и Telegram tool endpoints.

```bash
git clone https://github.com/maleon17/Codex-jarvis.git
cd Codex-jarvis
chmod +x setup.sh
./setup.sh
```

Скрипт создаёт отдельный `CODEX_HOME`, виртуальное окружение для MCP и
`config.toml` с локальными путями. Авторизация берётся из `$HOME/.codex/auth.json`
через symlink и не копируется в Git. Runtime-файлы (`codex_home/`, `state/`,
`.venv/`) игнорируются.

Для ручной установки можно скопировать `codex-jarvis.service.example`, заменить
`__USER__`, `__INSTALL_DIR__`, `__CODEX_HOME__` и `__CODEX_CWD__`, затем выполнить
`sudo systemctl daemon-reload && sudo systemctl enable --now codex-jarvis.service`.

## Загрузка userbot-модуля

Отправь `codex_ask.py` документом в выделенный тестовый Telegram-канал и
ответь на документ `.lm`. После загрузки в userbot доступны `.xask` и остальные
команды. Токены userbot/Telegram в этот репозиторий не входят.

После загрузки один раз выполни `.xasknet local <instance_id>` или
`.xasknet tailnet <instance_id> <backend_url>` для настройки сети конкретного
экземпляра. Эта настройка заменяет конфигурацию только через переменные окружения.

## Обновление

Чтобы переустановить загруженный модуль с последней версии из ветки `main`,
выполни:

```text
.dlm https://raw.githubusercontent.com/maleon17/Codex-jarvis/main/codex_ask.py
```

Настройки `.xasknet` хранятся в постоянной базе Heroku и при таком обновлении
не удаляются, поэтому повторная настройка не нужна.

## Проверка

```bash
python3 -m py_compile app_server.py codex_ask_watcher.py
systemctl status codex-jarvis
journalctl -u codex-jarvis -f
```

После загрузки модуля проверь обычный `.xask` и запрос, требующий реального
инструмента (`list_triggers`, `read_history` или `search_chat`). Финальный
ответ формируется только после результата инструмента, а не из предварительной
самоотчётности модели.

## Граница продуктов

`codex-telegram-bot` — отдельный Telegram Bot API интерфейс Codex. `Claude-jarvis`
— отдельный ClaudeAsk userbot/backend. Этот репозиторий — только CodexAsk
userbot/backend; общим остаётся лишь queue relay и namespace триггеров, потому
что это инфраструктурный транспорт и намеренно общее хранилище правил.

## Лицензия

MIT, см. `LICENSE`.
