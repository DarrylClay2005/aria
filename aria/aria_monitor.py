import asyncio
import logging
from core.autonomy import AutonomousEngine
from core.event_bus import EventBus
from core.override import override_manager

logger = logging.getLogger("discord")

class Monitor:
    def __init__(self, bot):
        self.engine = AutonomousEngine(bot)
        self.bus = EventBus(bot)
        self._last_full_scan = 0.0

    async def start(self):
        await self.bus.initialize()
        while True:
            try:
                await self.bus.sync_swarm_sources()
                if override_manager.autonomy_enabled:
                    await self.engine.run_pending_repairs()
                    await self.engine.run_pending_infra_tasks()
                    for event in await self.bus.claim_events(limit=50):
                        try:
                            await self.engine.handle_event(event)
                        finally:
                            await self.bus.mark_processed(event["id"])
                    now = asyncio.get_running_loop().time()
                    if now - self._last_full_scan >= 8.0:
                        await self.engine.run_once()
                        self._last_full_scan = now
            except Exception:
                logger.exception("[Monitor] Unhandled error in autonomous engine run.")
            await asyncio.sleep(2)
