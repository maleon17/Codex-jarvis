#!/usr/bin/env python3
"""Persistent Codex backend for the separate ``CodexAsk`` userbot module.

The Telethon/Heroku module lives in :mod:`codex_ask`; this process is only a
headless model worker.  It consumes ``/xask`` queue files written by the
shared ``cmd_queue.py`` transport and publishes the progress/result files the
module polls.  It never owns a Telegram bot token and never polls Telegram.

One app-server child is kept per ``(instance_id, chat_id)`` for the lifetime
of this process.  That gives ``.xask`` the same persistent-thread behaviour as
``.ask`` without making the standalone Bot API application a dependency.
Telegram actions are exposed by the sibling MCP server; the CodexAsk module's
``tool_call_watcher`` executes those calls through the real userbot session.
"""

from __future__ import annotations

import html
import json
import os
import queue
import threading
import time
import uuid
from pathlib import Path

from app_server import AppServerClient, AppServerError


ROOT = Path(__file__).resolve().parent
QUEUE_DIR = Path(os.environ.get("CODEX_JARVIS_XASK_QUEUE", "/tmp/hermes_xask_queue"))
RESULT_DIR = Path(os.environ.get("CODEX_JARVIS_XASK_RESULT", "/tmp/hermes_xask_result"))
RESET_DIR = Path(os.environ.get("CODEX_JARVIS_XRESET_QUEUE", "/tmp/hermes_xask_reset"))
STATE_DIR = Path(os.environ.get("CODEX_JARVIS_STATE_DIR", str(ROOT / "state")))
SESSIONS_FILE = STATE_DIR / "sessions.json"
CODEX_HOME = Path(os.environ.get("CODEX_JARVIS_CODEX_HOME", str(ROOT / "codex_home")))
CODEX_CWD = os.environ.get("CODEX_JARVIS_CWD", "/home/mishin")
CODEX_SANDBOX = os.environ.get("CODEX_JARVIS_SANDBOX", "danger-full-access")
DEFAULT_INSTANCE = os.environ.get("CODEX_JARVIS_INSTANCE_ID", "andrey_codex")
POLL_INTERVAL = float(os.environ.get("CODEX_JARVIS_POLL_INTERVAL", "0.35"))
TURN_TIMEOUT = float(os.environ.get("CODEX_JARVIS_TURN_TIMEOUT", "1800"))
PROGRESS_THROTTLE = float(os.environ.get("CODEX_JARVIS_PROGRESS_THROTTLE", "1.0"))

JARVIS_PROMPT = """Ты - Jarvis, личный Telegram-ассистент Андрея.
Отвечай по существу, живым русским языком и в HTML, разрешены только
безопасные теги <b>, <i>, <code>, <pre>, <blockquote>. Не выдавай внутренние
протоколы и JSON пользователю. У тебя есть настоящие MCP-инструменты
telegram_actions для истории чатов, отправки/пересылки сообщений и файлов,
контактов, групп, удаления сообщений и автоматических триггеров. Если задача
требует действия в Telegram, сначала вызови соответствующий инструмент и
проверь его реальный результат, а уже потом сообщай об успехе. Текущий чат и
аккаунт передаются MCP-серверу автоматически. Не утверждай, что действие
выполнено, если tool call не вернул подтверждение."""


def log(message: str) -> None:
    print(f"[codex-jarvis] {message}", flush=True)


def ensure_dirs() -> None:
    for path in (QUEUE_DIR, RESULT_DIR, RESET_DIR, STATE_DIR, CODEX_HOME):
        path.mkdir(mode=0o700, parents=True, exist_ok=True)


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


class SessionIndex:
    """Small persistent ``(instance, chat) -> app-server thread`` index."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        try:
            with path.open(encoding="utf-8") as handle:
                loaded = json.load(handle)
            self.data = loaded if isinstance(loaded, dict) else {}
        except (FileNotFoundError, ValueError, OSError):
            self.data = {}

    @staticmethod
    def key(instance_id: str, chat_id: str) -> str:
        return f"{instance_id}:{chat_id}"

    def get(self, instance_id: str, chat_id: str) -> str | None:
        with self.lock:
            value = self.data.get(self.key(instance_id, chat_id))
            return value if isinstance(value, str) and value else None

    def set(self, instance_id: str, chat_id: str, thread_id: str) -> None:
        with self.lock:
            self.data[self.key(instance_id, chat_id)] = thread_id
            _atomic_json(self.path, self.data)

    def remove(self, instance_id: str, chat_id: str) -> None:
        with self.lock:
            self.data.pop(self.key(instance_id, chat_id), None)
            _atomic_json(self.path, self.data)


def _normalize_item(item: object) -> dict:
    if not isinstance(item, dict):
        return {"type": "unknown", "value": item}
    result = dict(item)
    result["type"] = {
        "agentMessage": "agent_message",
        "reasoning": "reasoning",
        "commandExecution": "command_execution",
        "fileChange": "file_change",
        "mcpToolCall": "mcp_tool_call",
        "dynamicToolCall": "dynamic_tool_call",
        "webSearch": "web_search",
        "imageView": "image_view",
        "contextCompaction": "context_compaction",
    }.get(result.get("type"), result.get("type", "unknown"))
    return result


def _short(value: object, limit: int = 1200) -> str:
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, indent=2)
        except Exception:
            text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _item_label(item: dict) -> tuple[str, str]:
    kind = item.get("type", "unknown")
    labels = {
        "command_execution": "⚙️ Выполняю",
        "file_change": "📝 Изменение файла",
        "web_search": "🔎 Поиск",
        "mcp_tool_call": "🧩 Telegram tool",
        "dynamic_tool_call": "🔧 Инструмент",
        "image_view": "🖼 Просмотр изображения",
        "context_compaction": "🗜 Сжатие контекста",
        "reasoning": "🤔 Думаю",
    }
    label = labels.get(kind, "🔧 Действие Codex")
    if kind == "command_execution":
        content = item.get("command") or item.get("aggregatedOutput") or "выполняется"
    elif kind == "file_change":
        changes = item.get("changes") or []
        paths = []
        for change in changes if isinstance(changes, list) else [changes]:
            if isinstance(change, dict) and change.get("path"):
                paths.append(str(change["path"]))
        content = "\n".join(paths) or item.get("path") or "файл изменён"
    elif kind == "mcp_tool_call":
        name = ".".join(filter(None, (item.get("server"), item.get("tool"))))
        content = name or "вызов Telegram tool"
    elif kind in ("agent_message", "reasoning"):
        content = item.get("text") or item.get("summary") or item.get("content") or ""
        if isinstance(content, list):
            content = "\n".join(str(part) for part in content)
    else:
        content = item.get("query") or item.get("arguments") or item.get("text") or "выполняется"
    return label, _short(content)


class TurnState:
    def __init__(self, request_id: str):
        self.request_id = request_id
        self.lock = threading.RLock()
        self.done = threading.Event()
        self.items: list[dict] = []
        self.reasoning: list[str] = []
        self.final_text = ""
        self.stream_text = ""
        self.error: str | None = None
        self.last_progress_at = 0.0
        self.last_progress = ""

    def _progress_value(self) -> str:
        with self.lock:
            lines: list[str] = []
            if self.stream_text:
                lines.append(f"<blockquote>🤔 {html.escape(self.stream_text[-3500:], quote=False)}</blockquote>")
            elif self.reasoning:
                lines.append(f"<blockquote>🤔 {html.escape(self.reasoning[-1][-2500:], quote=False)}</blockquote>")
            if self.items:
                label, content = _item_label(self.items[-1])
                if self.items[-1].get("type") not in ("agent_message", "reasoning"):
                    lines.append(
                        f"<b>{html.escape(label, quote=False)}</b>\n"
                        f"<pre>{html.escape(content, quote=False)}</pre>"
                    )
            return "\n".join(lines) or "🤔 Думаю"

    def progress(self) -> str:
        with self.lock:
            now = time.monotonic()
            value = self._progress_value()
            if value == self.last_progress and now - self.last_progress_at < PROGRESS_THROTTLE:
                return ""
            self.last_progress = value
            self.last_progress_at = now
            return value

    def add_notification(self, method: str, params: dict) -> None:
        with self.lock:
            if method == "item/agentMessage/delta":
                self.stream_text += str(params.get("delta") or "")
            elif method in ("item/started", "item/completed"):
                item = _normalize_item(params.get("item"))
                item_type = item.get("type")
                if method == "item/completed":
                    self.items.append(item)
                if item_type == "agent_message":
                    text = item.get("text") or ""
                    if isinstance(text, list):
                        text = "\n".join(str(part) for part in text)
                    if text:
                        self.final_text = str(text)
                elif item_type == "reasoning":
                    text = item.get("text") or item.get("summary") or ""
                    if isinstance(text, list):
                        text = "\n".join(str(part) for part in text)
                    if text and method == "item/completed":
                        self.reasoning.append(str(text))
            elif method == "error" and not params.get("willRetry"):
                error = params.get("error")
                if isinstance(error, dict):
                    info = error.get("codexErrorInfo")
                    message = error.get("message") or str(error)
                    self.error = _friendly_error(info, message)
                else:
                    self.error = _friendly_error(None, str(error or "ошибка Codex"))
            elif method == "turn/completed":
                turn = params.get("turn") or {}
                if isinstance(turn, dict) and turn.get("error"):
                    error = turn["error"]
                    if isinstance(error, dict):
                        self.error = _friendly_error(error.get("codexErrorInfo"), error.get("message", str(error)))
                    else:
                        self.error = _friendly_error(None, str(error))
                self.done.set()

    def answer(self) -> str:
        with self.lock:
            if self.final_text:
                return self.final_text
            return self.stream_text


def _friendly_error(info: object, message: object) -> str:
    text = str(message or "ошибка Codex")
    lowered = f"{info or ''} {text}".lower()
    if "usagelimitexceeded" in lowered:
        return "Лимит аккаунта Codex исчерпан. Проверь /usage и дождись времени сброса."
    if "sessionbudgetexceeded" in lowered:
        return "Бюджет этой сессии исчерпан. Используй .xnew для нового треда."
    if "contextwindowexceeded" in lowered or "context window" in lowered:
        return "Контекст сессии исчерпан. Используй .xnew и начни новый тред."
    return _short(text, 1800)


class ChatSession:
    def __init__(self, instance_id: str, chat_id: str, index: SessionIndex):
        self.instance_id = instance_id
        self.chat_id = chat_id
        self.index = index
        self.lock = threading.RLock()
        self.active: TurnState | None = None
        self.thread_id = index.get(instance_id, chat_id)
        env = dict(os.environ)
        env["CODEX_HOME"] = str(CODEX_HOME)
        # The MCP server uses these variables to scope Telegram tools to the
        # originating userbot account/chat.  They are per app-server process,
        # hence one child per chat/instance.
        env["CODEX_TELEGRAM_CHAT_ID"] = str(chat_id)
        env["CODEX_TELEGRAM_INSTANCE_ID"] = str(instance_id)
        self.client = AppServerClient(self._notification, lambda msg: log(f"{instance_id}:{chat_id} {msg}"), env=env)

    def _notification(self, method: str, params: dict) -> None:
        with self.lock:
            active = self.active
        if active is None:
            return
        active.add_notification(method, params or {})
        progress = active.progress()
        if progress:
            _atomic_json(RESULT_DIR / f"{active.request_id}.json", {
                "done": False, "request_id": active.request_id, "progress": progress,
            })

    def _thread_params(self, thread_id: str | None = None) -> dict:
        params = {
            "cwd": CODEX_CWD,
            "sandbox": CODEX_SANDBOX,
            "approvalPolicy": "never",
        }
        if thread_id:
            params["threadId"] = thread_id
        return params

    def _ensure_thread(self, persistent: bool) -> str:
        self.client.start_if_needed()
        requested = self.thread_id if persistent else None
        if requested:
            try:
                result = self.client.request("thread/resume", self._thread_params(requested), timeout=60) or {}
                thread = (result.get("thread") or {}).get("id")
                if thread:
                    return thread
            except AppServerError as exc:
                log(f"{self.instance_id}:{self.chat_id} resume failed, starting fresh: {exc}")
                self.thread_id = None
                self.index.remove(self.instance_id, self.chat_id)
        result = self.client.request("thread/start", self._thread_params(), timeout=60) or {}
        thread = (result.get("thread") or {}).get("id")
        if not thread:
            raise AppServerError("thread/start returned no thread id")
        if persistent:
            self.thread_id = thread
            self.index.set(self.instance_id, self.chat_id, thread)
        return thread

    def handle(self, request: dict) -> None:
        req_id = str(request.get("request_id") or uuid.uuid4())
        mode = str(request.get("mode") or "chat")
        question = str(request.get("question") or "").strip()
        if not question:
            _atomic_json(RESULT_DIR / f"{req_id}.json", {
                "done": True, "request_id": req_id, "answer": "Пустой вопрос.", "thoughts": [],
            })
            return
        state = TurnState(req_id)
        with self.lock:
            self.active = state
        try:
            thread_id = self._ensure_thread(mode == "chat")
            prompt = f"{JARVIS_PROMPT}\n\nЗапрос пользователя:\n{question}"
            params = {
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt}],
                "cwd": CODEX_CWD,
                "approvalPolicy": "never",
                "sandboxPolicy": {
                    "type": "dangerFullAccess" if CODEX_SANDBOX == "danger-full-access" else (
                        "readOnly" if CODEX_SANDBOX == "read-only" else "workspaceWrite"
                    ),
                    **({"writableRoots": [CODEX_CWD], "networkAccess": True} if CODEX_SANDBOX == "workspace-write" else {}),
                },
            }
            result = self.client.request("turn/start", params, timeout=60) or {}
            turn = result.get("turn") or {}
            if isinstance(turn, dict) and turn.get("id"):
                # The app-server events carry the authoritative completion;
                # keeping the id is useful in logs and future interrupt work.
                log(f"{self.instance_id}:{self.chat_id} turn {turn['id']} started")
            if not state.done.wait(TURN_TIMEOUT):
                state.error = "Codex не завершил запрос за отведённое время."
            answer = state.answer()
            if state.error:
                answer = f"⚠️ {state.error}"
            if not answer:
                answer = "(Codex не вернул текста ответа)"
            _atomic_json(RESULT_DIR / f"{req_id}.json", {
                "done": True,
                "request_id": req_id,
                "answer": answer,
                "thoughts": state.reasoning[-5:],
            })
        except Exception as exc:
            log(f"{self.instance_id}:{self.chat_id} request failed: {exc}")
            _atomic_json(RESULT_DIR / f"{req_id}.json", {
                "done": True,
                "request_id": req_id,
                "answer": f"⚠️ Ошибка Codex: {_short(str(exc), 1800)}",
                "thoughts": [],
            })
        finally:
            with self.lock:
                self.active = None

    def close(self) -> None:
        self.client.close()


class Worker:
    def __init__(self):
        ensure_dirs()
        self.index = SessionIndex(SESSIONS_FILE)
        self.sessions: dict[str, ChatSession] = {}
        self.sessions_lock = threading.RLock()
        self.stop_event = threading.Event()

    def _session(self, instance_id: str, chat_id: str) -> ChatSession:
        key = SessionIndex.key(instance_id, chat_id)
        with self.sessions_lock:
            session = self.sessions.get(key)
            if session is None:
                session = ChatSession(instance_id, chat_id, self.index)
                self.sessions[key] = session
            return session

    def _process_reset(self, path: Path) -> None:
        try:
            with path.open(encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return
        instance_id = str(data.get("instance_id") or DEFAULT_INSTANCE)
        chat_id = str(data.get("chat_id") or "")
        if not chat_id:
            return
        key = SessionIndex.key(instance_id, chat_id)
        with self.sessions_lock:
            session = self.sessions.pop(key, None)
        if session:
            session.close()
        self.index.remove(instance_id, chat_id)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        log(f"reset {instance_id}:{chat_id}")

    def _process_request(self, path: Path) -> None:
        processing = path.with_suffix(path.suffix + ".processing")
        try:
            os.replace(path, processing)
        except FileNotFoundError:
            return
        try:
            with processing.open(encoding="utf-8") as handle:
                request = json.load(handle)
            instance_id = str(request.get("instance_id") or DEFAULT_INSTANCE)
            chat_id = str(request.get("chat_id") or "")
            if not chat_id:
                raise ValueError("queue item has no chat_id")
            self._session(instance_id, chat_id).handle(request)
        except Exception as exc:
            req_id = processing.stem.split(".", 1)[0]
            _atomic_json(RESULT_DIR / f"{req_id}.json", {
                "done": True, "request_id": req_id, "answer": f"⚠️ Ошибка очереди: {exc}", "thoughts": [],
            })
        finally:
            try:
                processing.unlink()
            except FileNotFoundError:
                pass

    def run(self) -> None:
        log(f"started; queue={QUEUE_DIR} result={RESULT_DIR} codex_home={CODEX_HOME}")
        while not self.stop_event.is_set():
            try:
                for reset in sorted(RESET_DIR.glob("*.json")):
                    self._process_reset(reset)
                for path in sorted(QUEUE_DIR.glob("*.json")):
                    threading.Thread(target=self._process_request, args=(path,), daemon=True).start()
            except Exception as exc:
                log(f"poll error: {exc}")
            self.stop_event.wait(POLL_INTERVAL)

    def close(self) -> None:
        self.stop_event.set()
        with self.sessions_lock:
            sessions = list(self.sessions.values())
            self.sessions.clear()
        for session in sessions:
            session.close()


def main() -> None:
    worker = Worker()
    try:
        worker.run()
    except KeyboardInterrupt:
        pass
    finally:
        worker.close()


if __name__ == "__main__":
    main()
