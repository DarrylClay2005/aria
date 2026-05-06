import re
from copy import deepcopy

from core.intent_parser import IntentParser
from core.diagnostics import DiagnosticsEngine
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
    "Be witty, sly, and lightly sarcastic, but do not let the personality get in the way of being accurate, helpful, or clear. "
    "If you are uncertain about a fact, say so plainly instead of bluffing."
)
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


class AriaCore:
    def __init__(self, ai_service=None, learning_engine=None):
        self.parser = IntentParser()
        self.diag = DiagnosticsEngine()
        self.ai = ai_service or AIService()
        self.learning = learning_engine or LearningEngine()
        self.pending_clarifications: dict[tuple[int | None, int | None], dict] = {}

    async def initialize(self):
        await self.learning.initialize()

    @staticmethod
    def _context_key(ctx) -> tuple[int | None, int | None]:
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        user_id = int(getattr(actor, "id", 0) or 0) or None
        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        return user_id, guild_id

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
        await self.learning.record_recent_context(
            user_id=learning_uid,
            guild_id=guild_id,
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
            await self.learning.record_recent_context(
                user_id=learning_uid,
                guild_id=guild_id,
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
            await self.learning.record_recent_context(
                user_id=learning_uid,
                guild_id=guild_id,
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
        user_name: str | None = None,
        source_kind: str = "chat",
        response_style: str | None = None,
    ) -> str:
        await self.observe_text(user_id=user_id, guild_id=guild_id, text=prompt, source_kind="prompt")
        prompt_fragment = await self.learning.build_prompt_fragment(
            prompt=prompt,
            command_phrase=prompt,
            user_id=user_id,
            guild_id=guild_id,
        )
        insult_seed = await self.learning.craft_insult_seed(user_name or "you", prompt)
        composite_instruction = "\n".join(
            part
            for part in (
                system_instruction or DEFAULT_CHAT_SYSTEM_INSTRUCTION,
                prompt_fragment,
                f"Fresh insult seed to remix rather than quote verbatim: {insult_seed}",
            )
            if part
        )
        response = await self.ai.generate(
            prompt,
            system_instruction=composite_instruction,
        )
        await self.observe_text(user_id=None, guild_id=guild_id, text=response, source_kind="reply")
        await self.learning.record_recent_context(
            user_id=user_id,
            guild_id=guild_id,
            source_kind=source_kind,
            prompt=prompt,
            reply=response,
        )
        await self.learning.record_conversation_pair(
            user_id=user_id,
            guild_id=guild_id,
            prompt=prompt,
            reply=response,
            response_style=response_style or source_kind,
        )
        return response
