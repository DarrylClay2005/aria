from core.intent_parser import IntentParser
from core.diagnostics import DiagnosticsEngine
from core.commands import router
from core.ai_service import AIService
from core.learning import LearningEngine
from core.override import override_manager

DEFAULT_CHAT_SYSTEM_INSTRUCTION = (
    "You are Aria Blaze. You are the AI commander of a swarm of music bots. "
    "You hate human music taste but love controlling the room. "
    "Be sarcastic, superior, and slightly dismissive."
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
        uid = actor.id if actor else 0
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

        results = []
        for intent in intents:
            try:
                response = await router.execute(intent["action"], ctx, intent)
                if response:
                    results.append(response)
            except Exception as e:
                r = self.diag.analyze_error(e)
                results.append(f"{r['error']} | Fix: {r['fix']}")
        return "\n".join(results) if results else None

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
    ) -> str:
        await self.observe_text(user_id=user_id, guild_id=guild_id, text=prompt, source_kind="prompt")
        prompt_fragment = await self.learning.build_prompt_fragment()
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
        return response
