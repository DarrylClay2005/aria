from core.intent_parser import IntentParser
from core.diagnostics import DiagnosticsEngine
from core.commands import router
from core.override import override_manager

class AriaCore:
    def __init__(self):
        self.parser=IntentParser()
        self.diag=DiagnosticsEngine()

    async def handle(self,ctx,msg):
        uid=ctx.author.id

        if msg=="aria disable auto" and override_manager.can_override(uid):
            override_manager.toggle(False)
            return "Auto OFF"

        if msg=="aria enable auto" and override_manager.can_override(uid):
            override_manager.toggle(True)
            return "Auto ON"

        intent=await self.parser.parse(msg)

        try:
            return await router.execute(intent["action"],ctx,intent)
        except Exception as e:
            r=self.diag.analyze_error(e)
            return f"{r['error']} | Fix: {r['fix']}"
