import asyncio
import logging
from core.autonomy import AutonomousEngine
from core.event_bus import EventBus
from core.override import override_manager

logger = logging.getLogger("discord")

class Monitor:
    def __init__(self, bot, bus=None):
        self.engine = AutonomousEngine(bot)
        self.bus = bus or getattr(bot, "event_bus", None)
        if self.bus is None:
            self.bus = EventBus(bot)
        self._last_full_scan = 0.0

    async def start(self):
        from core.webhooks import send_error_webhook_log, send_ops_webhook_log
        while True:
            try:
                await self.bus.sync_swarm_sources()
                if override_manager.autonomy_enabled:
                    await self.engine.run_pending_repairs()
                    await self.engine.run_pending_infra_tasks()
                    for event in await self.bus.claim_events(limit=50):
                        handled = False
                        error_text = None
                        try:
                            handled = await self.engine.handle_event(event)
                        except Exception as exc:
                            error_text = f"{type(exc).__name__}: {exc}"
                            logger.exception("[Monitor] Failed to handle swarm event %s.", event.get("id"))
                            try:
                                await send_error_webhook_log(
                                    "Aria Swarm Event Handler Error",
                                    error_text,
                                    fields=[
                                        ("Event ID", str(event.get("id", "n/a")), True),
                                        ("Event Type", str(event.get("event_type", "unknown"))[:128], True),
                                    ],
                                )
                            except Exception:
                                pass
                        try:
                            title = f"Swarm Event: {event.get('event_type','unknown')}"
                            desc = (
                                f"Bot: {event.get('bot_name','n/a')} | Guild: {event.get('guild_id','n/a')}\n"
                                f"Source: {event.get('source_system','n/a')}\n"
                                f"Severity: {event.get('severity','info')}\n"
                                f"Handled: {handled}\n"
                                f"Payload: {event.get('payload','{}')}"
                            )
                            fields = [("Handler Error", error_text[:1024], False)] if error_text else None
                            await send_ops_webhook_log(title, desc, fields=fields)
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
