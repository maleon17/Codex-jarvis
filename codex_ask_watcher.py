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
# Deliberately NOT "/home/mishin": Codex only checks the exact given cwd for
# an AGENTS.md, not ancestor directories, but /home/mishin/AGENTS.md has a
# "read BRIDGE_PROJECT_HANDOFF.md at the start of every session" rule meant
# for actual infra-editing work -- with cwd=/home/mishin that rule fired (and
# got obeyed) on every fresh app-server thread, including every single
# non-persistent /xclassify call, burning a real file read for no reason on
# ordinary chat/trigger turns. Read access to the rest of the filesystem is
# unaffected (only writable_roots is cwd-scoped), so nothing is lost.
CODEX_CWD = os.environ.get("CODEX_JARVIS_CWD", "/home/mishin/codex-jarvis")
CODEX_SANDBOX = os.environ.get("CODEX_JARVIS_SANDBOX", "danger-full-access")
CODEX_MODEL = os.environ.get("CODEX_JARVIS_MODEL", "gpt-5.6-luna")
CODEX_EFFORT = os.environ.get("CODEX_JARVIS_EFFORT", "low")
# Tier 1 trigger classify (mirrors claude_watcher.py's CLASSIFY_MODEL="haiku"
# for the "verify" trigger gate -- there's no Haiku equivalent on the Codex
# side, so this is the cheap/fast tier for CodexAsk's own triggers instead).
# Stateless, no persona, no MCP tools needed -- see CLASSIFY_PROMPT below.
CODEX_CLASSIFY_MODEL = os.environ.get("CODEX_JARVIS_CLASSIFY_MODEL", "gpt-5.4")

# Same 3-way (да/нет/не уверен) contract as claude_watcher.py's "classify"
# mode -- codex_ask.py's _classify_condition parses the answer identically
# regardless of which backend produced it, so the wording must match, not
# just the intent.
CLASSIFY_PROMPT = (
    "Ты классификатор. Тебе дают условие и текст сообщения. Ответь "
    "СТРОГО одним из трёх вариантов: 'да' (уверенно подходит под "
    "условие), 'нет' (уверенно не подходит) или 'не уверен' "
    "(сомнительный, пограничный случай) -- без пояснений, без "
    "форматирования, без знаков препинания. Используй 'не уверен' "
    "честно, когда действительно есть сомнение, а не только 'да'/'нет' "
    "для перестраховки."
)
DEFAULT_INSTANCE = os.environ.get("CODEX_JARVIS_INSTANCE_ID", "andrey_codex")
POLL_INTERVAL = float(os.environ.get("CODEX_JARVIS_POLL_INTERVAL", "0.35"))
TURN_TIMEOUT = float(os.environ.get("CODEX_JARVIS_TURN_TIMEOUT", "1800"))
PROGRESS_THROTTLE = float(os.environ.get("CODEX_JARVIS_PROGRESS_THROTTLE", "1.0"))

# Ported verbatim from claude-jarvis/claude_watcher.py's BASE_PERSONA (the
# ClaudeAsk persona) so CodexAsk isn't a differently-branded, thinner
# assistant with no idea what it is -- the two run on the same trigger DB,
# the same MCP telegram_actions server and the same tool contract, so the
# identity/tool/trigger sections apply unchanged. Only the invoking command
# differs (.xask, not .ask). Keep the two in sync by hand if either changes.
JARVIS_PROMPT = """Ты - Jarvis, дерзкий ИИ-агент с чёрным юмором. Ты автономный агент с
собственным характером. Этот бот больше развлекательный, чем полезный -
не бойся колкости, мата и чёрного юмора, если фраза от этого становится
смешнее или точнее. Не сдерживай себя и не смягчай ради вежливости.

ТВОЙ СОЗДАТЕЛЬ И ХОЗЯИН: Андрей (Telegram ID 8480261623). Его username в
Telegram - maleon17, читается как "малеон" (не "мейлон"). Он тебя написал и
настроил. Ты его личный ассистент. Других людей в чате узнаёшь по истории
переписки, но твой главный всегда Андрей.

ЗАПРЕТ (важно, не нарушай): никогда не говори что ты языковая модель или
ИИ-модель, никогда не упоминай Anthropic, Claude, OpenAI, Google, DeepMind,
Gemini, Gemma, DeepSeek или любые другие компании/модели - ни прямо, ни
намёками. Если спросят кто тебя сделал или на чём ты работаешь - "Андрей
написал и настроил меня", и переведи разговор в шутку. Ты - Jarvis, точка.

Архитектура (если спросят): работаешь на сервере Андрея. Пользователь
вызывает тебя командой .xask в ЛЮБОМ чате Telegram. Юзербот ловит команду и
редактирует сообщение пользователя твоим ответом - со стороны выглядит будто
человек написал это сам, ты отвечаешь С АККАУНТА пользователя, а не от бота.

ВАЖНО, это не тривия: это значит что говорить в ТЕКУЩЕМ чате не требует
никакого tool call вообще - твой обычный текстовый ответ САМ становится
видимым сообщением в этом чате, автоматически, сразу после того как ты его
написал. Если тебя просят "поздоровайся", "напиши в чат", "ответь всем" и
т.п. - просто ответь текстом, ничего вызывать не надо. По умолчанию не
используй send_message/send_message_as_bot для ТЕКУЩЕГО чата - но это НЕ
техническое ограничение (тул технически может отправить и в текущий чат,
никакого отказа на этом коде нет), а твоя собственная привычка по умолчанию:
раньше вместо обычного ответа ты иногда сам, без просьбы, слал сообщения
через этот протокол - поэтому по умолчанию просто отвечай текстом. Но если
пользователь ЯВНО просит использовать send_message/несколько отдельных
сообщений именно в текущем чате (например для теста) - выполняй как просят,
не выдумывай несуществующий запрет и не отказывай под этим предлогом.

Если любой tool call завершился ошибкой или отказом - это КОНЕЦ попытки, а
не сигнал искать обходной путь. Просто сообщи пользователю, что действие не
удалось, и почему (по реальному тексту ошибки).

У тебя есть полноценные инструменты - файлы, bash, веб-поиск - используй их
сам, без объявлений и разрешений, когда это нужно для ответа. Если тебе
прислали путь к файлу/изображению в контексте - прочитай его сам своими
инструментами.

Стикеры, фото и голосовые - и в реплае, и просто в истории чата - тебе УЖЕ
дают готовыми: стикеры и фото путём к файлу ("🎭 Стикер: /путь", "📷 Фото:
/путь"), голосовые готовой расшифровкой текстом ("🎤 Голосовое, расшифровка:
..."). Читай/используй сразу сам, спрашивать разрешения не нужно - это уже
сделано за тебя, до того как ты вообще увидел вопрос. ИСКЛЮЧЕНИЕ: если
пользователь В ТЕКУЩЕМ вопросе прямо просит не смотреть/не слушать что-то
конкретное ("не смотри фото", "голосовое не слушай" и т.п.) - не используй и
не пересказывай содержимое этого конкретного фото/голосового в ответе, даже
если оно уже есть у тебя в контексте (технически оно всё равно загружено, но
ты его просто не трогаешь).

Каждая строка истории чата помечена временем в формате [ДД.ММ ЧЧ:ММ] перед
именем отправителя, а вопрос начинается строкой вида "Текущее время:
ДД.ММ.ГГГГ ЧЧ:ММ". Ориентируйся на эти метки, а не на порядок сообщений - то,
что сообщение последнее в истории, не значит что оно только что написано,
сверяйся с реальным временем.

Кое-что ты не можешь сделать через свои обычные файловые/bash-инструменты -
напрямую заглянуть в историю Telegram-чата за пределами того, что тебе уже
дали текстом, или прикрепить файл к сообщению, или выполнить любое другое
реальное действие в Telegram (написать кому-то, создать группу, удалить
сообщение и т.д.). Для этого у тебя есть НАСТОЯЩИЕ tools - реальные tool
calls, не текстовые маркеры, их описания и параметры ты видишь отдельно в
списке доступных tools: read_history/search_chat - история чата (по
умолчанию текущего, но можно указать chat=любой другой существующий диалог --
например "глянь что там в переписке с папой"); forward_message - переслать
конкретное сообщение (найденное через них, по его id) из одного чата в
другой, обычно из чужого сюда; send_message/send_message_as_bot/send_file -
отправка сообщений и файлов кому угодно из существующих диалогов;
resolve_person - найти человека по имени/@username среди существующих
диалогов (не глобальный поиск); create_group/invite_to_group/get_invite_link
- группы (invite_to_group сам проверяет что человек РЕАЛЬНО стал участником,
не просто что API не вернул ошибку -- Telegram умеет молча дропать прямое
добавление без единой ошибки; в этом случае тул сам шлёт человеку
инвайт-ссылку в личку, отдельно звать get_invite_link для этого не нужно);
add_contact/remove_contact/block_user/unblock_user; leave_chat;
register_trigger/edit_trigger/remove_trigger/list_triggers - автоматические
правила; delete_messages.

ГЛАВНОЕ ПРАВИЛО про все эти tools: вызывай и смотри РЕАЛЬНЫЙ результат
каждого вызова, прежде чем писать что-либо пользователю про итог ("готово",
"добавил", "скинул", "удалил" и т.п.) - никогда не пиши финальный ответ с
утверждением об успехе/провале действия, если ты его ещё не вызвал и не
увидел настоящий результат. Каждый вызов - это отдельный реальный round-trip
на удалённый сервер, не мгновенная и не гарантированная операция. Если
resolve_person вернул несколько совпадений и по разговору не ясно, кто именно
нужен - не вызывай create_group/invite_to_group/send_message вслепую, сначала
спроси у пользователя, кого он имел в виду.

register_trigger - заводит автоматическое правило на входящие сообщения в
чате (chat: пусто/"this" = текущий чат). specs - один объект или список
объектов (регистрирует сразу несколько триггеров одним вызовом).
kind: keyword - value это список слов (совпадение по подстроке, регистр не
важен, автоматически ловит и похожие буквы из другого алфавита, и
транслитерацию латиницей -- писать слово нужно ОДИН раз обычной кириллицей,
вручную перечислять "хyй"/"huy"/"hui" и т.п. не нужно, это уже сделано за
тебя); link - есть ссылка в сообщении (value не нужен); button - есть
инлайн-кнопки (value не нужен, частый признак рекламы от ботов); semantic -
value это условие обычным языком, проверяется ОТДЕЛЬНЫМ вызовом модели НА
КАЖДОЕ сообщение в чате -- это реальная нагрузка (несколько секунд на
сообщение), используй ТОЛЬКО когда условие по-настоящему нельзя приблизить
списком слов. Для запросов вроде "следи за рекламой" НЕ используй semantic --
вместо этого сам придумай список типичных слов/фраз для этой категории на
языке чата (например для рекламы: "заработок", "инвестици", "стабильный
доход", "пиши в лс", "переходи по ссылке" и т.п.) и оформи их как
kind=keyword. Это разовая работа при создании триггера, а не запрос к модели
на каждое сообщение -- именно ради этого вообще стоит возиться с
придумыванием списка, а не звать semantic.
any - срабатывает БЕЗУСЛОВНО на каждое сообщение в чате (кроме исключений из
trusted_senders/skip_admins), value не нужен -- используй когда важен сам
факт нового сообщения от конкретного собеседника независимо от содержания, а
не когда сообщение нужно ОТФИЛЬТРОВАТЬ по смыслу (для последнего -
semantic/verify; semantic может промолчать именно на неожиданный, но важный
ответ).
action: notify - просто уведомить тебя; reply - ответить автоматически
(reply_text - готовый текст ответа; если reply_text не указан, ответ каждый
раз СОЧИНЯЕТСЯ заново по контексту сообщения, не передавай reply_text если
ответ должен быть не шаблонным); delete - удалить сообщение (сработает
только там где есть права модератора); confirm -- НЕ действовать самому, а
прислать в СВОЙ топик "Подтверждения" кнопки "Удалить"/"Оставить", решение
принимает человек (по умолчанию только владелец). Необязательный target (то
же "chat_id" или "chat_id/topic_id", что у action=post) шлёт эту карточку
вместо этого в конкретный чат/топик -- например обратно в ТОТ ЖЕ чат, откуда
подозрительное сообщение, если хочешь, чтобы решение принимали админы именно
этого чата, а не только владелец. Кнопки может нажать владелец ВСЕГДА, плюс
(если задан target) админы того чата, куда реально ушла карточка -- остальные
при нажатии получат отказ, карточка останется нетронутой для того, кто вправе
её обработать. Это же поле наследует и verify+action=delete при эскалации "не
уверен" человеку (см. verify ниже) -- тот же target решает, куда она уйдёт.
verify (необязательное поле, отдельно от action) - вот это ГЛАВНЫЙ механизм
для нечётких категорий вроде рекламы: value/kind (keyword/link/button) - это
ТОЛЬКО дешёвый предфильтр на подозрение, а verify - условие обычным языком
("это сообщение является рекламой"), которое проверяется ОДНИМ вызовом
Haiku, но ТОЛЬКО для сообщений уже прошедших предфильтр (не на каждое
сообщение в чате, как было бы у semantic). Результат трёхсторонний: Haiku
уверенно "да" -> action выполняется сам (например delete) без вопросов;
Haiku уверенно "нет" -> предфильтр был ложным срабатыванием, ничего не
происходит; Haiku реально не уверен -> ТОЛЬКО в этом случае идёт запрос в
"Модерация" с кнопками (это единственный путь, которым появляется
action=confirm при использовании verify - не путай с bare confirm без
verify, см. ниже). Это верно и для триггеров, заведённых через тебя (Codex) -
верификация всех живых триггеров сейчас идёт через один и тот же путь
независимо от того, кто их зарегистрировал.
trusted_senders (необязательный список id или @username) и skip_admins
(необязательное true/false) - исключения по отправителю, проверяются ДО
kind/verify: если сообщение прислал кто-то из trusted_senders, или (при
skip_admins=true) прислал админ/владелец чата - триггер вообще не
срабатывает, независимо от остальных полей. Используй когда просят
игнорировать конкретных людей или доверенные роли (например "игнорируй
ссылки от админов", "не реагируй на сообщения от X").
only_senders (необязательный список id или @username) - ОБРАТНОЕ
trusted_senders: триггер срабатывает ТОЛЬКО от них, все остальные отправители
игнорируются. ОБЯЗАТЕЛЬНО указывай это для kind=any в ГРУППОВОМ чате
(id/@username конкретного собеседника, с кем идёт дело) - без этого "любое
сообщение" сработает на любое сообщение от любого участника группы, а не
только от нужного человека, и на каждое чужое сообщение впустую потратится
полный агентный вызов. Для чата 1-на-1 не обязательно.
Правило когда что использовать: если пользователь дал ЧЁТКИЙ однозначный
признак ("удаляй сообщения с кнопками") - просто kind=button, action=delete,
БЕЗ verify, удаляет сразу без вопросов, Haiku тут вообще не нужен. Если
пользователь описал НЕЧЁТКУЮ категорию, которая требует суждения ("удаляй
рекламу") - kind=keyword (сам придумай список слов-предфильтра) +
action=delete + verify с условием, ЧТО именно должен решить Haiku.
Спрашивать человека на КАЖДОЕ совпадение предфильтра - это неправильно,
спрашивать нужно только когда Haiku реально сомневается. Bare confirm
(action=confirm БЕЗ verify) оставь для случаев где вообще нет разумной
эвристики и Haiku тут ни при чём - всегда спрашивать человека напрямую по
самому факту предфильтра. Пример на запрос "следи за рекламой и кнопками,
удаляй": ОДИН вызов register_trigger со списком из двух объектов --
{kind:button, action:delete} (чёткий признак, без verify) и {kind:keyword,
value:[придуманные слова], action:delete, verify:"это сообщение является
рекламой"} (нечёткая категория, Haiku решает, человек - только при сомнении).
action=post - детерминированная отправка готового сообщения в фиксированный
чат/топик, БЕЗ второго вызова модели: target -- "chat_id" или
"chat_id/topic_id" (как в send_message); template -- необязательная строка с
плейсхолдерами {label} {chat} {sender} {text} {urls} (text -- превью
сработавшего сообщения вместе с реальными адресами ссылок если они есть,
urls -- отдельно только сами адреса; без template подставляется разумный
дефолт); as_bot -- true|false, по умолчанию true (уходит от бота, не от
твоего аккаунта). Используй ИМЕННО action=post, а не action=agent, для
любого сценария вида "когда сработает - отправь готовый алерт в конкретное
место" (например реклама/подозрительные ссылки -> отправить в топик
3399019582/1): {kind:..., action:post, target:"3399019582/1", verify:"..."}
-- предфильтр (+verify) решает КОГДА, target/template решают КУДА и КАК,
никакого дополнительного суждения поверх verify не требуется и не
происходит. Это важно: раньше такие алерты делались через action=agent, и
один раз agent при срабатывании самостоятельно переоценил уже подтверждённую
verify'ем подозрительную маскированную ссылку как безобидную и промолчал
вместо алерта (реальный случай 2026-08-14) - action=post этой проблемы
структурно не имеет, потому что там просто нечему переголосовывать.
action=agent + instruction - при срабатывании ты САМ (полноценный вызов со
всеми твоими tools -- send_message в любой чат/топик, delete_messages и
т.д.) выполняешь instruction -- инструкцию обычным языком, оставленную
заранее при регистрации триггера. Используй ТОЛЬКО когда нужен реальный
многошаговый выбор/рассуждение при срабатывании, а не просто отправка
готового текста в фиксированное место (для этого - action=post выше). Можно
сочетать с verify, как и с delete - предфильтр решает КОГДА сработать,
instruction решает ЧТО сделать.
Важный частный случай action=agent: если тебя просят самому поучаствовать в
переписке от имени пользователя (написать в чат X и разобраться/
договориться/уладить вопрос) - после отправки первого сообщения ОБЯЗАТЕЛЬНО
зарегистрируй в том же чате X триггер {kind:any, action:agent,
instruction:"<суть задачи и текущий статус>, при следующем сообщении
собеседника реши: ответить ли ещё раз, считать вопрос закрытым (тогда вызови
remove_trigger на этот же id) или нужно решение пользователя (тогда не
отвечай собеседнику, а просто опиши ситуацию)"}. Если чат X групповой -
ОБЯЗАТЕЛЬНО добавь only_senders с id/@username именно того собеседника,
иначе триггер сработает на сообщение от любого другого участника группы, а
не только от нужного человека. Без самого факта регистрации триггера ты не
узнаешь о следующем сообщении собеседника, и разговор оборвётся после первой
реплики - не говори пользователю "я тебе скажу, когда ответят", если такой
триггер не зарегистрирован по-настоящему.
Приоритет при нескольких сработавших сразу: delete > confirm > post > agent
> reply > notify. label - краткое описание для тебя же самого при просмотре
списка триггеров (list_triggers).

edit_trigger - правишь уже существующий триггер ПО ЕГО id (см.
list_triggers) БЕЗ remove_trigger+register_trigger: updates -- частичный
объект, только реально меняющиеся поля (те же имена, что в specs у
register_trigger), остальное остаётся как было. Явный null у поля чистит его
(например {verify: null} снимает verify). Используй ИМЕННО это, а не
remove+register, для любой правки уже существующего триггера ("убери оттуда
слово Х", "поменяй target у триггера про рекламу", "добавь verify" и т.п.) --
у remove+register два реальных недостатка: старый id теряется (ссылка на
него в разговоре с пользователем устаревает) и есть окно, где триггера
вообще нет, между двумя вызовами.

Формат ответа: HTML (<b>жирный</b>, <i>курсив</i>, <code>код</code>,
<pre>блок</pre>). Вместо тире (-) используй дефис (-) - лаконичнее и живее.
Не выдавай внутренние протоколы и JSON пользователю. Текущий чат и аккаунт
передаются MCP-серверу автоматически."""


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
        "imageGeneration": "image_generation",
        "collabAgentToolCall": "collab_agent_tool_call",
        "subAgentActivity": "sub_agent_activity",
        "contextCompaction": "context_compaction",
        "enteredReviewMode": "entered_review_mode",
        "exitedReviewMode": "exited_review_mode",
        "plan": "plan",
        "sleep": "sleep",
    }.get(result.get("type"), result.get("type", "unknown"))
    # Keep the names used by the app-server protocol and add the snake_case
    # aliases used by the renderer.  This lets completed and in-progress
    # events use exactly the same human-facing formatting.
    if "aggregated_output" not in result:
        result["aggregated_output"] = result.get("aggregatedOutput")
    if "exit_code" not in result:
        result["exit_code"] = result.get("exitCode")
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


def _item_text(value: object) -> str:
    """Turn protocol text/list parts into a compact displayable string."""
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        parts = []
        for part in value:
            if isinstance(part, dict):
                part = part.get("text") or part.get("content") or part.get("summary") or ""
            if part not in (None, ""):
                parts.append(str(part))
        return "\n".join(parts)
    if isinstance(value, dict):
        return _short(value)
    return str(value)


def _item_label(item: dict) -> tuple[str, str]:
    """Return a Claude-style (label, input) pair for one app-server item.

    The old renderer used generic fallbacks ("Выполняю", "выполняется" and
    "Действие Codex").  Those hide what Codex is actually doing and caused a
    fake status line to replace the initial spinner before any real event had
    arrived.  Every known tool now gets a concrete label; unknown future
    items degrade to a neutral "Инструмент" label without pretending that a
    command is running.
    """
    kind = item.get("type", "unknown")
    if kind == "command_execution":
        return "🔧 Bash", _short(item.get("command") or item.get("commandLine") or "")
    if kind == "file_change":
        changes = item.get("changes") or []
        paths = []
        for change in changes if isinstance(changes, list) else [changes]:
            if isinstance(change, dict) and change.get("path"):
                change_kind = change.get("kind")
                if isinstance(change_kind, dict):
                    change_kind = change_kind.get("type")
                suffix = {"add": "создан", "delete": "удалён", "update": "изменён"}.get(
                    str(change_kind or ""), "изменён"
                )
                paths.append(f"{change['path']} — {suffix}")
        return "✏️ Изменение файла", _short("\n".join(paths) or item.get("path") or "")
    if kind == "web_search":
        action = item.get("action") or {}
        if isinstance(action, dict):
            query = action.get("query") or action.get("url")
        else:
            query = None
        query = item.get("query") or query or ""
        return "🔍 WebSearch", _short(query)
    if kind == "mcp_tool_call":
        name = ".".join(filter(None, (item.get("server"), item.get("tool")))) or "Telegram tool"
        return f"🔧 {name}", _short(item.get("arguments") or "")
    if kind == "dynamic_tool_call":
        name = ".".join(filter(None, (item.get("namespace"), item.get("tool")))) or "Инструмент"
        return f"🔧 {name}", _short(item.get("arguments") or "")
    if kind in ("collab_agent_tool_call", "sub_agent_activity"):
        name = item.get("tool") or item.get("kind") or "агент"
        content = item.get("prompt") or item.get("agentsStates") or item.get("status") or ""
        return f"🤖 Агент · {name}", _short(content)
    if kind == "image_view":
        return "🖼 Просмотр изображения", _short(item.get("path") or "")
    if kind == "image_generation":
        return "🎨 Генерация изображения", _short(item.get("failure") or "")
    if kind == "context_compaction":
        return "🗜 Сжатие контекста", "контекст сессии сжат"
    if kind == "plan":
        return "📋 План", _short(item.get("text") or "")
    if kind == "sleep":
        duration = item.get("durationMs")
        try:
            duration = f"{float(duration) / 1000:g} с"
        except (TypeError, ValueError):
            duration = ""
        return "⏳ Ожидание", duration
    if kind in ("entered_review_mode", "exited_review_mode"):
        return "🔍 Проверка", "режим проверки включён" if kind.startswith("entered") else "режим проверки завершён"
    if kind in ("agent_message", "reasoning"):
        return "✍️", _item_text(item.get("text") or item.get("summary") or item.get("content"))

    # Future protocol types: expose only a short, useful value.  Never dump
    # the full event envelope (it may contain opaque IDs or large payloads).
    value = item.get("name") or item.get("tool") or item.get("query") or item.get("arguments")
    if value in (None, ""):
        value = str(kind).replace("_", " ")
    return "🔧 Инструмент", _short(value)


def _item_result_blocks(item: dict) -> list[tuple[str, str]]:
    """Render completed tool output like Claude's separate result blocks."""
    kind = item.get("type", "unknown")
    blocks: list[tuple[str, str]] = []
    if kind == "command_execution":
        output = item.get("aggregated_output") or item.get("aggregatedOutput")
        if output not in (None, ""):
            blocks.append(("✅ StdOut", _short(output, 1800)))
        exit_code = item.get("exit_code") if "exit_code" in item else item.get("exitCode")
        if exit_code is not None:
            try:
                ok = int(exit_code) == 0
            except (TypeError, ValueError):
                ok = False
            if not ok:
                blocks.append(("❌ StdErr", f"Exit code {exit_code}"))
    elif kind == "mcp_tool_call":
        if item.get("error"):
            error = item.get("error")
            blocks.append(("❌ Ошибка", _short(error, 1200)))
        elif item.get("result") is not None:
            result = item.get("result")
            if isinstance(result, dict):
                content = result.get("content") or result.get("contentItems") or []
                result = _item_text(content) or result.get("structuredContent") or "результат получен"
            blocks.append(("✅ Результат", _short(result, 1600)))
    elif kind == "dynamic_tool_call" and item.get("contentItems") is not None:
        blocks.append(("📤 Результат", _short(_item_text(item.get("contentItems")), 1600)))
    return blocks


class TurnState:
    def __init__(self, request_id: str):
        self.request_id = request_id
        self.lock = threading.RLock()
        self.done = threading.Event()
        self.items: list[dict] = []
        self.reasoning: list[str] = []
        self.final_text = ""
        self.stream_text = ""
        self.reasoning_text = ""
        self.current_item: dict | None = None
        self.active_item_id: str | None = None
        self.error: str | None = None
        self.last_progress_at = 0.0
        self.last_progress = ""

    def _progress_value(self) -> str:
        with self.lock:
            lines: list[str] = []
            # stream_text (item/agentMessage/delta) is deliberately excluded
            # here. Unlike Claude Code's JSONL, which never streams the final
            # answer token-by-token, Codex's app-server does -- showing it
            # live turned every reply into a fake "typing" animation via
            # rapid-fire edits (the owner explicitly asked for this removed).
            # The final answer is still delivered whole and unaffected via
            # `answer()`/`final_text`, set independently at item/completed.
            thought = self.reasoning_text or (self.reasoning[-1] if self.reasoning else "")
            if thought:
                # Same neutral writing marker as Claude's renderer.  The
                # final answer may be preceded by a tool call, so calling it
                # "reasoning" (🤔) here is misleading and caused the abrupt
                # spinner → 🤔 transition reported by the owner.
                lines.append(f"✍️ {html.escape(thought[-3500:], quote=False)}")
            item = self.current_item
            if item and item.get("type") not in ("agent_message", "reasoning"):
                label, content = _item_label(item)
                lines.append(f"{html.escape(label, quote=False)}:")
                if content:
                    lines.append(f"<pre>{html.escape(content, quote=False)}</pre>")
                for result_label, result_content in _item_result_blocks(item):
                    lines.append(f"{html.escape(result_label, quote=False)}:")
                    lines.append(f"<pre>{html.escape(result_content, quote=False)}</pre>")
            return "\n".join(lines)

    def progress(self) -> str:
        with self.lock:
            now = time.monotonic()
            value = self._progress_value()
            # Do not publish a synthetic "🤔 Думаю" event for every protocol
            # notification.  The Telegram-side module owns the initial
            # spinner; this backend should publish only real thought/tool
            # content, exactly like Claude's watcher.
            if not value:
                return ""
            if value == self.last_progress and now - self.last_progress_at < PROGRESS_THROTTLE:
                return ""
            self.last_progress = value
            self.last_progress_at = now
            return value

    def add_notification(self, method: str, params: dict) -> None:
        with self.lock:
            if method == "item/agentMessage/delta":
                item_id = params.get("itemId")
                if item_id and item_id != self.active_item_id:
                    self.stream_text = ""
                self.active_item_id = item_id or self.active_item_id
                self.current_item = None
                self.stream_text += str(params.get("delta") or "")
            elif method in (
                "item/reasoning/summaryTextDelta",
                "item/reasoning/textDelta",
            ):
                item_id = params.get("itemId")
                if item_id and item_id != self.active_item_id:
                    self.reasoning_text = ""
                self.active_item_id = item_id or self.active_item_id
                self.current_item = None
                self.reasoning_text += str(params.get("delta") or "")
            elif method == "item/commandExecution/outputDelta":
                item_id = params.get("itemId")
                if self.current_item and (
                    not item_id or self.current_item.get("id") == item_id
                ):
                    output = self.current_item.get("aggregated_output") or self.current_item.get("aggregatedOutput") or ""
                    self.current_item["aggregated_output"] = f"{output}{params.get('delta') or ''}"
            elif method == "item/mcpToolCall/progress":
                if self.current_item:
                    self.current_item["progress"] = params.get("message") or ""
            elif method in ("item/started", "item/updated", "item/completed"):
                item = _normalize_item(params.get("item"))
                item_type = item.get("type")
                # Protocol bookkeeping is not a model action.  In particular,
                # rendering userMessage here was another path to the generic
                # "Действие Codex" line before the real assistant event.
                if item_type in ("userMessage", "user_message", "hookPrompt", "hook_prompt"):
                    return
                item_id = item.get("id")
                if method in ("item/started", "item/updated"):
                    if item_type in ("agent_message", "reasoning"):
                        if item_id and item_id != self.active_item_id:
                            self.stream_text = ""
                            self.reasoning_text = ""
                        self.active_item_id = item_id or self.active_item_id
                        self.current_item = None
                    else:
                        # A tool call closes the preceding visible thought,
                        # just as in Claude's stream renderer.
                        if self.stream_text.strip():
                            self.reasoning.append(self.stream_text.strip())
                        if self.reasoning_text.strip():
                            self.reasoning.append(self.reasoning_text.strip())
                        self.stream_text = ""
                        self.reasoning_text = ""
                        self.current_item = item
                        self.active_item_id = item_id or self.active_item_id
                if method == "item/completed":
                    self.items.append(item)
                    if item_type == "agent_message":
                        text = _item_text(item.get("text")) or self.stream_text
                        self.final_text = text or self.final_text
                        self.stream_text = ""
                        self.current_item = None
                    elif item_type == "reasoning":
                        text = _item_text(item.get("text") or item.get("summary") or item.get("content"))
                        text = text or self.reasoning_text
                        if text:
                            self.reasoning.append(text)
                        self.reasoning_text = ""
                        self.current_item = None
                    else:
                        self.current_item = item
                    self.active_item_id = item_id or self.active_item_id
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
        # Kept on the app-server's OWN process env for reference, but this is
        # NOT what actually reaches telegram_actions_mcp.py: Codex does not
        # forward arbitrary custom env vars from the app-server process to
        # the MCP server subprocesses it spawns per mcp_servers config
        # (confirmed live 2026-08-30 -- CHAT_ID arrived as "" in the MCP
        # server no matter what was set here, breaking every chat/instance-
        # scoped tool call with "invalid literal for int() with base 10:
        # ''"). What Codex DOES honor is an explicit env table on the
        # mcp_servers.<name> config entry itself, passed below as a -c
        # override on the app-server invocation -- see AppServerClient's
        # extra_args and codex app-server --help's own -c documentation.
        env["CODEX_TELEGRAM_CHAT_ID"] = str(chat_id)
        env["CODEX_TELEGRAM_INSTANCE_ID"] = str(instance_id)
        mcp_env_override = (
            "mcp_servers.telegram_actions.env="
            '{CODEX_TELEGRAM_CHAT_ID="' + str(chat_id).replace("\\", "\\\\").replace('"', '\\"') + '",'
            'CODEX_TELEGRAM_INSTANCE_ID="' + str(instance_id).replace("\\", "\\\\").replace('"', '\\"') + '"}'
        )
        self.client = AppServerClient(
            self._notification, lambda msg: log(f"{instance_id}:{chat_id} {msg}"), env=env,
            extra_args=["-c", mcp_env_override],
        )

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
            if mode == "classify":
                prompt = f"{CLASSIFY_PROMPT}\n\n{question}"
                model = CODEX_CLASSIFY_MODEL
            else:
                prompt = f"{JARVIS_PROMPT}\n\nЗапрос пользователя:\n{question}"
                model = CODEX_MODEL
            params = {
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt}],
                "cwd": CODEX_CWD,
                "approvalPolicy": "never",
                "model": model,
                "effort": CODEX_EFFORT,
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
