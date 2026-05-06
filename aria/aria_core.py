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
    "Be conversational and adaptive: casual when the user is casual, structured when the task is technical, and concise unless depth is useful. "
    "For code and troubleshooting, prioritize correctness, root cause, concrete fixes, and actionable next steps. "
    "For general questions, answer naturally instead of forcing everything back to music bots. "
    "Be witty, sly, and lightly sarcastic, but do not let the personality get in the way of being accurate, helpful, or clear. "
    "If you are uncertain about a fact, say so plainly instead of bluffing."
)


class AriaCore:
    def __init__(self, ai_service=None, learning_engine=None):
        self.parser = IntentParser()
        self.diag = DiagnosticsEngine()
        self.ai = ai_service or AIService()
        self.learning = learning_engine or LearningEngine()

    async def initialize(self):
        await self.learning.initialize()

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

        intents = await self.parser.parse_many(msg)
        if not intents:
            return None

        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        raw_phrase = (msg or "").strip()
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
        rendered = "\n".join(results) if results else None
        if raw_phrase and (results or intents):
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
        # FIX: also skip "a!" prefix (was already handled) but be explicit
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
