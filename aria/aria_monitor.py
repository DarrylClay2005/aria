import asyncio
import logging
from core.autonomy import AutonomousEngine
from core.override import override_manager

logger = logging.getLogger("discord")

class Monitor:
    def __init__(self, bot):
        self.engine = AutonomousEngine(bot)

    async def start(self):
        while True:
            try:
                if override_manager.autonomy_enabled:
                    await self.engine.run_once()
            except Exception:
                # FIX: log exceptions instead of silently crashing the monitor loop
                logger.exception("[Monitor] Unhandled error in autonomous engine run.")
            await asyncio.sleep(5)
