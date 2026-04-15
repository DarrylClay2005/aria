import asyncio
from core.autonomy import AutonomousEngine
from core.override import override_manager

class Monitor:
    def __init__(self,bot):
        self.engine=AutonomousEngine(bot)

    async def start(self):
        while True:
            if override_manager.autonomy_enabled:
                await self.engine.run_once()
            await asyncio.sleep(5)
