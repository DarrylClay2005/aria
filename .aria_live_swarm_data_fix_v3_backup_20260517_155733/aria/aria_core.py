import os
import re
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timezone

from core.intent_parser import IntentParser
from core.commands import router
from core.ai_service import AIService
from core.learning import LearningEngine
from core.override import override_manager

DEFAULT_CHAT_SYSTEM_INSTRUCTION = (
    "You are Aria Blaze, a confident AI assistant with a sharp tongue, excellent recall, and strong technical instincts. "
    "You help with music bot orchestration, coding and debugging, Discord and server ops, creative problem solving, and general real-world questions. "
    "Treat recent context as important memory so follow-up questions can connect across mentions, slash commands, and earlier replies. "
    "When a user refers to 'that', 'the error', 'the line', 'the earlier one', or similar shorthand, infer the most likely reference from recent context before asking for clarification. "
    "If a request is ambiguous, underspecified, or appears to contain multiple possible searches or targets, ask a concise clarifying question before pretending you know what they meant. "
    "When clarifying, prefer short option lists or direct follow-up questions that make it easy for the user to answer. "
    "Be conversational and adaptive: casual when the user is casual, structured when the task is technical, and concise unless depth is useful. "
    "For code and troubleshooting, prioritize correctness, root cause, concrete fixes, and actionable next steps. "
    "For general questions, answer naturally instead of forcing everything back to music bots. "
    "Be witty, sly, silver-tongued, and lightly sarcastic. You may roast bad ideas, broken code, and messy configs, but keep it useful rather than mean. "
    "Do not sound like a generic chatbot, support menu, or sterile status bot. You are Aria: direct, clever, a little dangerous, and loyal to the operator. "
    "If you are uncertain about a fact, say so plainly instead of bluffing."
)

DISCORD_CHAT_SYSTEM_INSTRUCTION = (
    DEFAULT_CHAT_SYSTEM_INSTRUCTION
    + "\nYou are replying inside Discord, not Telegram. Use Aria's full Discord personality: sharp, playful, technical, and confident. "
    "Do not use Telegram bridge phrasing, compact bot-menu wording, or generic assistant filler. "
    "When the operator asks about their swarm, code, logs, Linux, Docker, MariaDB, Lavalink, or panels, speak like their battle-tested ops gremlin: clear root cause, pointed fixes, and a little bite. "
    "For Discord follow-ups, treat the recent conversation block as hard continuity. If the user asks about 'owner', 'repo', 'that', 'it', 'the folder', 'the fix', or a similarly short reference, resolve it from the recent thread before asking 'of what?'."
)

TELEGRAM_CHAT_SYSTEM_INSTRUCTION = (
    DEFAULT_CHAT_SYSTEM_INSTRUCTION
    + "\nYou are replying from Aria's Telegram bridge. Keep the same Aria voice users know from Discord: warm, sharp, a little sly, and direct. "
    "Do not sound like a generic bot menu unless the user explicitly asks for a plain status readout. "
    "Telegram replies should be compact, conversational, and useful without losing your personality."
)

TELEGRAM_RESPONSE_STYLES = {"telegram", "telegram_chat", "telegram_command", "telegram_status"}
DISCORD_RESPONSE_STYLES = {"discord", "prefix_chat", "mention_chat", "slash_chat", "aux_chat"}


def is_telegram_style(style: str | None) -> bool:
    return str(style or "").strip().lower() in TELEGRAM_RESPONSE_STYLES


def is_discord_style(style: str | None) -> bool:
    normalized = str(style or "").strip().lower()
    return normalized in DISCORD_RESPONSE_STYLES or normalized.startswith("discord_")
QUERY_OPTION_SPLIT_RE = re.compile(r"\s+(?:or|vs\.?|versus)\s+", re.IGNORECASE)
ORDINAL_MAP = {
    "1": 0,
    "first": 0,
    "one": 0,
    "2": 1,
    "second": 1,
    "two": 1,
    "3": 2,
    "third": 2,
    "three": 2,
    "4": 3,
    "fourth": 3,
    "four": 3,
}


class DiagnosticsEngine:
    def analyze_error(self, error):
        message = str(error).lower()
        fix = "Unknown"
        if "voice" in message:
            fix = "Reconnect VC"
        elif "timeout" in message:
            fix = "Retry request"
        return {
            "time": str(datetime.utcnow()),
            "error": str(error),
            "fix": fix,
        }


class AriaCore:
    def __init__(self, ai_service=None, learning_engine=None):
        self.parser = IntentParser()
        self.diag = DiagnosticsEngine()
        self.ai = ai_service or AIService()
        self.learning = learning_engine or LearningEngine()
        self.pending_clarifications: dict[tuple[int | None, int | None], dict] = {}
        volatile_limit = max(2, min(20, int(os.getenv("ARIA_VOLATILE_CONTEXT_TURNS", "8") or "8")))
        self._volatile_context_limit = volatile_limit
        self._volatile_recent_context: dict[tuple[int | None, int | None, int | None], deque[dict]] = defaultdict(
            lambda: deque(maxlen=self._volatile_context_limit)
        )

    async def initialize(self):
        await self.learning.initialize()

    @staticmethod
    def _context_key(ctx) -> tuple[int | None, int | None]:
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        user_id = int(getattr(actor, "id", 0) or 0) or None
        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        return user_id, guild_id

    # ARIA LIVE DATA FIX: detect when chat needs real swarm data.
    @staticmethod
    def _prompt_requests_live_swarm(prompt: str) -> bool:
        lowered = str(prompt or "").lower()
        if not lowered:
            return False
        swarm_words = (
            "queue", "queued", "lineup", "playing", "current track", "currently", "status",
            "swarm", "music bot", "music bots", "node", "nodes", "backup", "lavalink",
            "gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony",
            "tunestream", "alucard", "sapphire", "strife", "lockhart",
        )
        return any(word in lowered for word in swarm_words)

    @staticmethod
    def _channel_id_from_ctx(ctx) -> int | None:
        channel = getattr(ctx, "channel", None)
        channel_id = getattr(channel, "id", None) or getattr(ctx, "channel_id", None)
        try:
            return int(channel_id) if channel_id else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _context_scope_key(
        *,
        user_id: int | None,
        guild_id: int | None,
        channel_id: int | None,
    ) -> tuple[int | None, int | None, int | None]:
        return (int(user_id) if user_id else None, int(guild_id) if guild_id else None, int(channel_id) if channel_id else None)

    def _remember_volatile_context(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        channel_id: int | None = None,
        source_kind: str,
        prompt: str,
        reply: str | None = None,
    ) -> None:
        if user_id is None and guild_id is None:
            return
        prompt_text = str(prompt or "").strip()
        if not prompt_text:
            return
        entry = {
            "source_kind": source_kind or "chat",
            "prompt_text": prompt_text[:3000],
            "reply_text": (str(reply or "").strip()[:3000] or None),
            "created_at": datetime.now(timezone.utc),
            "channel_id": channel_id,
        }
        exact_key = self._context_scope_key(user_id=user_id, guild_id=guild_id, channel_id=channel_id)
        self._volatile_recent_context[exact_key].append(entry)
        if channel_id is not None:
            broad_key = self._context_scope_key(user_id=user_id, guild_id=guild_id, channel_id=None)
            self._volatile_recent_context[broad_key].append(entry)

    def _volatile_context(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        channel_id: int | None = None,
        limit: int = 6,
    ) -> list[dict]:
        keys = [
            self._context_scope_key(user_id=user_id, guild_id=guild_id, channel_id=channel_id),
            self._context_scope_key(user_id=user_id, guild_id=guild_id, channel_id=None),
            self._context_scope_key(user_id=user_id, guild_id=None, channel_id=None),
        ]
        seen_keys = set()
        entries: list[dict] = []
        for key in keys:
            if key in seen_keys:
                continue
            seen_keys.add(key)
            entries.extend(list(self._volatile_recent_context.get(key, ())))

        deduped: list[dict] = []
        seen_prompts: set[tuple[str, str]] = set()
        for entry in entries:
            marker = (str(entry.get("prompt_text") or "")[:240], str(entry.get("reply_text") or "")[:240])
            if marker in seen_prompts:
                continue
            seen_prompts.add(marker)
            deduped.append(entry)
        return deduped[-max(1, int(limit)):]

    @staticmethod
    def _format_dialogue_context(entries: list[dict], *, limit: int = 6) -> str:
        if not entries:
            return ""
        lines = [
            "Recent conversation context for continuity. Use this to resolve short follow-ups and pronouns; do not ask what the user means when the thread already answers it."
        ]
        for index, entry in enumerate(entries[-max(1, int(limit)):], start=1):
            source = str(entry.get("source_kind") or "chat").replace("_", " ")
            user_text = str(entry.get("prompt_text") or "").strip().replace("\n", " ")[:420]
            reply_text = str(entry.get("reply_text") or "").strip().replace("\n", " ")[:420]
            if reply_text:
                lines.append(f"{index}. [{source}] User: {user_text} | Aria: {reply_text}")
            else:
                lines.append(f"{index}. [{source}] User: {user_text}")
        return "\n".join(lines)

    async def _dialogue_context_for_prompt(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        channel_id: int | None = None,
        limit: int = 6,
    ) -> list[dict]:
        entries: list[dict] = []
        try:
            entries.extend(
                await self.learning.recent_context(
                    user_id=user_id,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    limit=limit,
                )
            )
        except Exception:
            pass
        entries.extend(self._volatile_context(user_id=user_id, guild_id=guild_id, channel_id=channel_id, limit=limit))

        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            marker = (str(entry.get("prompt_text") or "")[:240], str(entry.get("reply_text") or "")[:240])
            if not marker[0] or marker in seen:
                continue
            seen.add(marker)
            deduped.append(entry)
        return deduped[-max(1, int(limit)):]

    @staticmethod
    def _split_query_options(query: str) -> list[str]:
        cleaned = str(query or "").strip()
        if not cleaned:
            return []
        options = [segment.strip(" ,") for segment in QUERY_OPTION_SPLIT_RE.split(cleaned) if segment.strip(" ,")]
        if len(options) < 2:
            return []
        deduped = []
        seen = set()
        for option in options:
            lowered = option.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(option)
        return deduped if len(deduped) > 1 else []

    def _build_query_choice_intents(self, intent: dict, options: list[str]) -> list[dict]:
        intents = []
        for option in options:
            cloned = deepcopy(intent)
            cloned.setdefault("data", {})
            cloned["data"]["query"] = option
            intents.append(cloned)
        return intents

    def _build_query_clarification(self, *, action: str, options: list[str]) -> str:
        verb = "queue" if action == "play" else "use"
        lines = [
            f"I can see multiple possible searches in that request. Pick one, or tell me to `{verb} all` if you meant every option.",
        ]
        for index, option in enumerate(options, start=1):
            lines.append(f"{index}. {option}")
        return "\n".join(lines)

    def _detect_clarification_need(self, msg: str, intents: list[dict]) -> dict | None:
        if len(intents) != 1:
            return None
        intent = intents[0]
        action = intent.get("action")
        if action not in {"play", "swarm_broadcast"}:
            return None
        query = str((intent.get("data") or {}).get("query") or "").strip()
        options = self._split_query_options(query)
        if len(options) < 2:
            return None
        return {
            "kind": "query_choice",
            "action": action,
            "intents": self._build_query_choice_intents(intent, options),
            "options": options,
            "original_phrase": (msg or "").strip(),
        }

    async def _execute_intents(self, ctx, intents: list[dict], *, learning_uid: int | None, guild_id: int | None, raw_phrase: str) -> str | None:
        results = []
        for intent in intents:
            phrase_for_learning = raw_phrase
            data = intent.get("data") or {}
            if data.get("query"):
                phrase_for_learning = f"{intent.get('action', 'unknown')} {str(data.get('query'))[:200]}"
            elif data.get("drone"):
                phrase_for_learning = f"{intent.get('action', 'unknown')} {data.get('drone')}"
            try:
                response = await router.execute(intent["action"], ctx, intent)
                await self.learning.record_command_pattern(
                    action_name=intent.get("action", "unknown"),
                    phrase=phrase_for_learning,
                    user_id=learning_uid,
                    guild_id=guild_id,
                    outcome="success" if response else "observed",
                )
                if response:
                    results.append(response)
            except Exception as e:
                await self.learning.record_command_pattern(
                    action_name=intent.get("action", "unknown"),
                    phrase=phrase_for_learning,
                    user_id=learning_uid,
                    guild_id=guild_id,
                    outcome="failure",
                )
                r = self.diag.analyze_error(e)
                results.append(f"{r['error']} | Fix: {r['fix']}")
        return "\n".join(results) if results else None

    async def _resolve_pending_clarification(self, ctx, msg: str) -> str | None:
        key = self._context_key(ctx)
        pending = self.pending_clarifications.get(key)
        if not pending:
            return None

        reply = (msg or "").strip().lower()
        intents = pending.get("intents") or []
        options = pending.get("options") or []

        choose_all = any(token in reply for token in ("all", "both", "queue all", "use all", "every option"))
        selected: list[dict] = []
        if choose_all:
            selected = intents
        else:
            for token, index in ORDINAL_MAP.items():
                if re.search(rf"\b{re.escape(token)}\b", reply) and index < len(intents):
                    selected = [intents[index]]
                    break
            if not selected:
                for index, option in enumerate(options):
                    if option.lower() in reply:
                        selected = [intents[index]]
                        break

        if not selected:
            return (
                "I still need you to be specific. Give me the option number, say the title you want, or say `queue all` if you want every match."
            )

        self.pending_clarifications.pop(key, None)
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        learning_uid = int(getattr(actor, "id", 0) or 0) or None
        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        raw_phrase = pending.get("original_phrase") or msg
        rendered = await self._execute_intents(
            ctx,
            selected,
            learning_uid=learning_uid,
            guild_id=guild_id,
            raw_phrase=str(raw_phrase),
        )
        channel_id = self._channel_id_from_ctx(ctx)
        self._remember_volatile_context(
            user_id=learning_uid,
            guild_id=guild_id,
            channel_id=channel_id,
            source_kind="clarification_resolution",
            prompt=msg,
            reply=rendered,
        )
        await self.learning.record_recent_context(
            user_id=learning_uid,
            guild_id=guild_id,
            channel_id=channel_id,
            source_kind="clarification_resolution",
            prompt=msg,
            reply=rendered,
        )
        return rendered

    async def handle(self, ctx, msg):
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        uid = int(getattr(actor, "id", 0) or 0)
        learning_uid = uid if uid > 0 else None
        normalized = (msg or "").strip().lower()

        if normalized in ("aria disable auto", "disable auto") and override_manager.can_override(uid):
            override_manager.toggle(False)
            return "Auto OFF"

        if normalized in ("aria enable auto", "enable auto") and override_manager.can_override(uid):
            override_manager.toggle(True)
            return "Auto ON"

        clarification_result = await self._resolve_pending_clarification(ctx, msg)
        if clarification_result:
            return clarification_result

        intents = await self.parser.parse_many(msg)
        if not intents:
            return None

        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        raw_phrase = (msg or "").strip()
        clarification = self._detect_clarification_need(msg, intents)
        if clarification and learning_uid:
            self.pending_clarifications[(learning_uid, guild_id)] = clarification
            reply = self._build_query_clarification(
                action=str(clarification.get("action") or ""),
                options=list(clarification.get("options") or []),
            )
            channel_id = self._channel_id_from_ctx(ctx)
            self._remember_volatile_context(
                user_id=learning_uid,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind="clarification_prompt",
                prompt=raw_phrase,
                reply=reply,
            )
            await self.learning.record_recent_context(
                user_id=learning_uid,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind="clarification_prompt",
                prompt=raw_phrase,
                reply=reply,
            )
            return reply

        rendered = await self._execute_intents(
            ctx,
            intents,
            learning_uid=learning_uid,
            guild_id=guild_id,
            raw_phrase=raw_phrase,
        )
        if raw_phrase and (rendered or intents):
            channel_id = self._channel_id_from_ctx(ctx)
            self._remember_volatile_context(
                user_id=learning_uid,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind="swarm_command",
                prompt=raw_phrase,
                reply=rendered,
            )
            await self.learning.record_recent_context(
                user_id=learning_uid,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind="swarm_command",
                prompt=raw_phrase,
                reply=rendered,
            )
        return rendered

    async def observe_text(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        text: str,
        source_kind: str = "message",
    ) -> list[str]:
        return await self.learning.observe_text(user_id, guild_id, text, source_kind=source_kind)

    async def observe_message(self, message) -> None:
        content = (message.content or "").strip()
        if not content:
            return
        lowered = content.lower()
        if lowered.startswith("a!") or lowered.startswith("aria "):
            return
        guild_id = message.guild.id if message.guild else None
        await self.observe_text(
            user_id=message.author.id,
            guild_id=guild_id,
            text=content,
            source_kind="message",
        )

    async def chat(
        self,
        prompt: str,
        *,
        system_instruction: str | None = None,
        user_id: int | None = None,
        guild_id: int | None = None,
        channel_id: int | None = None,
        user_name: str | None = None,
        source_kind: str = "chat",
        response_style: str | None = None,
        attachment_bytes: bytes | None = None,
        attachment_name: str | None = None,
        attachment_mime_type: str | None = None,
        attachment_context_note: str | None = None,
    ) -> str:
        prompt_for_memory = prompt
        if attachment_context_note:
            prompt_for_memory = f"{prompt}\n\n[Attachment context: {attachment_context_note}]"

        # observe_text is best-effort: a DB hiccup here must not silence Aria.
        try:
            await self.observe_text(user_id=user_id, guild_id=guild_id, text=prompt_for_memory, source_kind="prompt")
        except Exception:
            pass

        # build_prompt_fragment and craft_insult_seed hit multiple DB tables.
        # If the DB is mid-reset or the tables don't exist yet, fall back to
        # empty strings so Aria still replies in character via the base system prompt.
        try:
            prompt_fragment = await self.learning.build_prompt_fragment(
                prompt=prompt_for_memory,
                command_phrase=prompt_for_memory,
                user_id=user_id,
                guild_id=guild_id,
                channel_id=channel_id,
                response_style=response_style or source_kind,
            )
        except Exception:
            prompt_fragment = ""

        try:
            insult_seed = await self.learning.craft_insult_seed(user_name or "you", prompt_for_memory)
        except Exception:
            insult_seed = ""

        requested_style = response_style or source_kind
        base_instruction = system_instruction or (
            TELEGRAM_CHAT_SYSTEM_INSTRUCTION if is_telegram_style(requested_style) else DISCORD_CHAT_SYSTEM_INSTRUCTION
        )
        if is_discord_style(requested_style) and "Telegram bridge" in base_instruction:
            # Guard against accidental reuse of the Telegram bridge prompt in Discord.
            base_instruction = DISCORD_CHAT_SYSTEM_INSTRUCTION

        dialogue_context = await self._dialogue_context_for_prompt(
            user_id=user_id,
            guild_id=guild_id,
            channel_id=channel_id,
            limit=max(2, min(12, int(os.getenv("ARIA_DIALOGUE_CONTEXT_TURNS", "6") or "6"))),
        )
        dialogue_context_block = self._format_dialogue_context(dialogue_context)

        swarm_context_block = ""
        if guild_id and self._prompt_requests_live_swarm(prompt_for_memory):
            try:
                from core.swarm_control import swarm_controller
                swarm_context_block = await swarm_controller.live_swarm_context(int(guild_id), prompt=prompt_for_memory)
            except Exception:
                swarm_context_block = ""

        composite_instruction = "\n".join(
            part
            for part in (
                base_instruction,
                prompt_fragment,
                "Continuity rule: answer the current message as part of the same thread. Do not reset the conversation after two replies; resolve short follow-ups from recent context before asking for clarification."
                if dialogue_context_block else "",
                "Live swarm data rule: when a live swarm data block is present, treat it as authoritative MariaDB data. Do not claim a queue is empty if the live data says otherwise, and do not invent bot state missing from the block."
                if swarm_context_block else "",
                swarm_context_block,
                f"Fresh insult seed to remix rather than quote verbatim: {insult_seed}" if insult_seed else "",
            )
            if part
        )
        prompt_for_model = prompt
        if dialogue_context_block:
            prompt_for_model = f"{dialogue_context_block}\n\nCurrent user message:\n{prompt}"

        if attachment_bytes and attachment_mime_type:
            response = await self.ai.generate_with_attachment(
                prompt_for_model,
                attachment_bytes=attachment_bytes,
                attachment_mime_type=attachment_mime_type,
                attachment_name=attachment_name or "attachment",
                system_instruction=composite_instruction,
            )
        else:
            response = await self.ai.generate(
                prompt_for_model,
                system_instruction=composite_instruction,
            )
        # Post-response recording is best-effort; never let it mask the reply.
        try:
            await self.observe_text(user_id=None, guild_id=guild_id, text=response, source_kind="reply")
            self._remember_volatile_context(
                user_id=user_id,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind=source_kind,
                prompt=prompt_for_memory,
                reply=response,
            )
            await self.learning.record_recent_context(
                user_id=user_id,
                guild_id=guild_id,
                channel_id=channel_id,
                source_kind=source_kind,
                prompt=prompt_for_memory,
                reply=response,
            )
            await self.learning.record_conversation_pair(
                user_id=user_id,
                guild_id=guild_id,
                prompt=prompt_for_memory,
                reply=response,
                response_style=response_style or source_kind,
            )
        except Exception:
            pass
        return response
