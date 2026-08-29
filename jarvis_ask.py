# JarvisAsk — shared dispatcher for the ClaudeAsk and CodexAsk backends.
#
# The two backend modules deliberately remain separate: each owns its model
# queue, progress renderer, and MCP tool poller.  This small common module is
# the single place that knows which backend is preferred for a trigger and
# which one is the safe fallback when the preferred account is unavailable.

import re

from herokutl.tl.custom import Message

from .. import loader


ENGINE_CLAUDE = "claude"
ENGINE_CODEX = "codex"
ENGINES = (ENGINE_CLAUDE, ENGINE_CODEX)

# These are user-visible worker errors, not ordinary model prose.  A trigger
# request must be retried on the other subscription when one of these is
# returned; an arbitrary answer containing a word like "лимит" must not be
# treated as a transport failure.
BACKEND_FAILURE_RE = re.compile(
    r"(?:лимит аккаунта|usage limit|rate limit|rate_limit|quota|исчерпан|"
    r"hit your limit|you(?:'|’)ve hit|не завершил запрос|"
    r"ошиб(?:ка|ки) (?:воркера|codex)|ошибка подключения|таймаут|timeout|"
    r"worker error|service unavailable|internal server error)",
    re.IGNORECASE,
)


@loader.tds
class JarvisAsk(loader.Module):
    """Common owner-aware coordinator for both Jarvis model backends."""

    strings = {"name": "JarvisAsk"}

    def _backend(self, engine):
        """Return the loaded backend module for ``engine`` if available."""
        name = "ClaudeAsk" if engine == ENGINE_CLAUDE else "CodexAsk"
        try:
            return self.lookup(name)
        except Exception:
            return None

    def backend(self, engine):
        engine = str(engine or ENGINE_CLAUDE).lower()
        return self._backend(engine)

    def fallback_engine(self, engine):
        return ENGINE_CODEX if str(engine).lower() == ENGINE_CLAUDE else ENGINE_CLAUDE

    def fallback(self, engine):
        return self._backend(self.fallback_engine(engine))

    @staticmethod
    def is_failure(answer):
        return isinstance(answer, str) and bool(BACKEND_FAILURE_RE.search(answer))

    def engine_for_trigger(self, trigger, default=ENGINE_CLAUDE):
        engine = str((trigger or {}).get("engine") or default).lower()
        return engine if engine in ENGINES else default

    async def client_ready(self):
        # The backend modules own both active Telegram watchers.  Keeping
        # this coordinator free of a third watcher is intentional: both
        # watchers call into the same owner field and therefore cannot execute
        # one trigger twice.
        return

    def _get_triggers(self):
        # ClaudeAsk historically owns this DB namespace; keeping the same
        # namespace is what makes migration lossless for existing rules.
        return self.db.get("ClaudeAsk", "triggers", {})

    async def handle_message(self, message, owner, backend=None):
        """Single shared trigger dispatcher called by both active watchers.

        Each backend watcher passes its owner name, so both watchers may stay
        enabled without double-firing: a rule is consumed only by the
        watcher matching its ``engine`` field. Matching/action helpers remain
        on the backend modules because they need that backend's Telegram
        session, queue, and progress implementation.
        """
        if not isinstance(message, Message) or message.out:
            return
        owner = str(owner or ENGINE_CLAUDE).lower()
        backend = backend or self.backend(owner)
        if backend is None:
            backend = self.fallback(owner)
            if backend is None:
                return
            owner = self.fallback_engine(owner)
        chat_triggers = self._get_triggers().get(str(message.chat_id))
        if not chat_triggers:
            return
        resolved = []
        for trigger in chat_triggers:
            engine = self.engine_for_trigger(trigger)
            if engine != owner:
                continue
            if await backend._is_trigger_exempt(trigger, message):
                continue
            if not await backend._trigger_matches(trigger, message):
                continue
            if trigger.get("verify"):
                action = await backend._resolve_verified_action(trigger, message)
                if action == "none":
                    continue
                resolved.append({**trigger, "action": action})
            else:
                resolved.append(trigger)
        if resolved:
            await backend._fire_triggers(resolved, message)
