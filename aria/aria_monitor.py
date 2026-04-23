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
        from core.webhooks import send_ops_webhook_log
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
                            # --- Swarm Ops Feed: Send all processed events ---
                            try:
                                title = f"Swarm Event: {event.get('event_type','unknown')}"
                                desc = f"Bot: {event.get('bot_name','n/a')} | Guild: {event.get('guild_id','n/a')}\nSource: {event.get('source_system','n/a')}\nSeverity: {event.get('severity','info')}\nPayload: {event.get('payload','{}')}"
                                await send_ops_webhook_log(title, desc)
                            except Exception as wh_exc:
                                logger.warning(f"[Monitor] Failed to send ops webhook: {wh_exc}")
                        finally:
                            await self.bus.mark_processed(event["id"])
                    now = asyncio.get_running_loop().time()
                    if now - self._last_full_scan >= 8.0:
                        await self.engine.run_once()
                        self._last_full_scan = now
            except Exception:
                logger.exception("[Monitor] Unhandled error in autonomous engine run.")
            await asyncio.sleep(2)
