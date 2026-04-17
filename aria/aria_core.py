from core.intent_parser import IntentParser
from core.diagnostics import DiagnosticsEngine
from core.commands import router
from core.ai_service import AIService
from core.override import override_manager

DEFAULT_CHAT_SYSTEM_INSTRUCTION = (
    "You are Aria Blaze. You are the AI commander of a swarm of music bots. "
    "You hate human music taste but love controlling the room. "
    "Be sarcastic, superior, and slightly dismissive."
)


class AriaCore:
    def __init__(self, ai_service=None):
        self.parser=IntentParser()
        self.diag=DiagnosticsEngine()
        self.ai = ai_service or AIService()

    async def handle(self,ctx,msg):
        uid=ctx.author.id

        if msg=="aria disable auto" and override_manager.can_override(uid):
            override_manager.toggle(False)
            return "Auto OFF"

        if msg=="aria enable auto" and override_manager.can_override(uid):
            override_manager.toggle(True)
            return "Auto ON"

        if not override_manager.autonomy_enabled:
            return None

        intent=await self.parser.parse(msg)
        if not intent or intent.get("action")=="unknown":
            return None

        try:
            return await router.execute(intent["action"],ctx,intent)
        except Exception as e:
            r=self.diag.analyze_error(e)
            return f"{r['error']} | Fix: {r['fix']}"

    async def chat(self, prompt: str, *, system_instruction: str | None = None) -> str:
        return await self.ai.generate(
            prompt,
            system_instruction=system_instruction or DEFAULT_CHAT_SYSTEM_INSTRUCTION,
        )
