import asyncio
import logging
import os
import time
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
        self._ops_event_cache: dict[str, dict[str, float | int]] = {}
        self._ops_event_cooldown_seconds = max(
            30.0,
            float(os.getenv("ARIA_OPS_EVENT_COOLDOWN_SECONDS", "120") or "120"),
        )

    @staticmethod
    def _trim(value, limit: int = 900) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    @staticmethod
    def _display_guild(guild_id) -> str:
        return str(guild_id) if guild_id else "not provided"

    @staticmethod
    def _severity_color(severity: str):
        level = str(severity or "info").lower()
        if level == "error":
            return 0xED4245
        if level == "warning":
            return 0xFEE75C
        return 0x5865F2

    def _event_notice_key(self, event: dict, payload: dict) -> str:
        event_type = str(event.get("event_type") or "unknown")
        bot_name = str(event.get("bot_name") or "swarm")
        guild_id = str(event.get("guild_id") or 0)
        if event_type == "bot_error_logged":
            category = str(payload.get("error_category") or payload.get("error_type") or "unknown")
            track_query = str(payload.get("track_query") or "").strip().lower()
            summary = str(payload.get("error_summary") or "").strip().lower()
            return f"{event_type}:{bot_name}:{guild_id}:{category}:{track_query or summary}"[:255]
        if event_type == "recoverable_state_detected":
            return (
                f"{event_type}:{bot_name}:{guild_id}:{payload.get('queue_count', 0)}:"
                f"{payload.get('backup_count', 0)}:{int(bool(payload.get('current_track')))}"
            )[:255]
        if event_type == "playback_state_drift":
            return f"{event_type}:{bot_name}:{guild_id}:{payload.get('home_vc_id') or 0}"[:255]
        if event_type == "health_trending_down":
            return f"{event_type}:{bot_name}:{guild_id}:{payload.get('status_label') or 'unknown'}"[:255]
        return f"{event_type}:{bot_name}:{guild_id}"[:255]

    def _reserve_ops_notice(self, key: str) -> int | None:
        now = time.time()
        entry = self._ops_event_cache.get(key)
        if not entry:
            self._ops_event_cache[key] = {"last_sent": now, "suppressed": 0}
            return 0
        last_sent = float(entry.get("last_sent") or 0.0)
        if now - last_sent < self._ops_event_cooldown_seconds:
            entry["suppressed"] = int(entry.get("suppressed") or 0) + 1
            return None
        suppressed = int(entry.get("suppressed") or 0)
        entry["last_sent"] = now
        entry["suppressed"] = 0
        return suppressed

    def _should_publish_event(self, event: dict) -> bool:
        event_type = str(event.get("event_type") or "")
        severity = str(event.get("severity") or "info").lower()
        if event_type == "bot_state_changed" and severity == "info":
            return False
        return True

    def _format_event_notification(self, event: dict, handled: bool, error_text: str | None, repeat_count: int):
        payload = dict(event.get("payload") or {})
        event_type = str(event.get("event_type") or "unknown")
        bot_name = str(event.get("bot_name") or "swarm")
        guild_display = self._display_guild(event.get("guild_id"))
        severity = str(event.get("severity") or "info").lower()
        created_at = self._trim(event.get("created_at") or payload.get("created_at") or "n/a", 120)
        action_text = "auto-response triggered" if handled else "logged for review only"
        fields = [
            ("Bot", bot_name, True),
            ("Guild", guild_display, True),
            ("Severity", severity, True),
            ("Observed", created_at, True),
            ("Aria Action", action_text, True),
        ]
        if repeat_count > 0:
            fields.append(("Recent Repeats", str(repeat_count), True))
        if error_text:
            fields.append(("Handler Error", self._trim(error_text, 1024), False))

        if event_type == "bot_error_logged":
            category = str(payload.get("error_category") or payload.get("error_type") or "generic_python_error")
            summary = self._trim(payload.get("error_summary") or payload.get("error_message") or "Bot error logged.", 1024)
            title_map = {
                "lavalink_search_failure": "Swarm Error: Track Lookup Failed",
                "voice_connect_timeout": "Swarm Error: Voice Connect Timed Out",
                "recovery_retries_exhausted": "Swarm Error: Recovery Retries Exhausted",
                "presence_update_failed": "Swarm Error: Presence Update Failed",
                "generic_python_error": "Swarm Error: Bot Runtime Error",
            }
            description_map = {
                "lavalink_search_failure": f"{bot_name} could not resolve a requested track through Lavalink.",
                "voice_connect_timeout": f"{bot_name} timed out while trying to join or restore a voice session.",
                "recovery_retries_exhausted": f"{bot_name} ran out of automatic recovery attempts for a voice session.",
                "presence_update_failed": f"{bot_name} hit a Discord presence update failure.",
                "generic_python_error": f"{bot_name} logged a runtime error that may need operator review.",
            }
            title = title_map.get(category, "Swarm Error: Bot Runtime Error")
            description = description_map.get(category, f"{bot_name} logged a runtime error.")
            track_query = self._trim(payload.get("track_query") or "", 1024)
            if track_query:
                fields.append(("Track Query", track_query, False))
            fields.append(("Summary", summary, False))
            fields.append(("Raw Error", self._trim(payload.get("error_message") or "", 1024), False))
            return title, description, fields, self._severity_color(severity)

        if event_type == "recoverable_state_detected":
            description = f"{bot_name} has queue or playback state that looks recoverable, but it is not actively playing."
            fields.extend([
                ("Queue Items", str(payload.get("queue_count", 0)), True),
                ("Backup Items", str(payload.get("backup_count", 0)), True),
                ("Home VC", str(payload.get("home_vc_id") or "missing"), True),
                ("Track Present", "yes" if payload.get("current_track") else "no", True),
            ])
            return "Swarm Recovery Candidate", description, fields, self._severity_color(severity)

        if event_type == "playback_state_drift":
            description = f"{bot_name} looks like it is playing without a stable voice anchor."
            fields.extend([
                ("Home VC", str(payload.get("home_vc_id") or "missing"), True),
                ("Queue Items", str(payload.get("queue_count", 0)), True),
                ("Backup Items", str(payload.get("backup_count", 0)), True),
            ])
            return "Swarm Drift Warning", description, fields, self._severity_color(severity)

        if event_type == "health_trending_down":
            description = f"{bot_name} is trending downward compared to its recent swarm health history."
            recent = payload.get("recent_health") or []
            fields.extend([
                ("Health", self._trim(payload.get("health_score") or "n/a", 64), True),
                ("Status", self._trim(payload.get("status_label") or "unknown", 64), True),
                ("Recent Scores", self._trim(", ".join(str(v) for v in recent[:4]) if recent else "n/a", 1024), False),
            ])
            return "Swarm Health Trending Down", description, fields, self._severity_color(severity)

        description = (
            f"{bot_name} emitted `{event_type}` from `{event.get('source_system') or 'unknown'}`."
        )
        fields.append(("Payload", self._trim(payload, 1024), False))
        return f"Swarm Event: {event_type}", description, fields, self._severity_color(severity)

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
                            if self._should_publish_event(event):
                                payload = dict(event.get("payload") or {})
                                notice_key = self._event_notice_key(event, payload)
                                repeat_count = self._reserve_ops_notice(notice_key)
                                if repeat_count is not None:
                                    title, desc, fields, color = self._format_event_notification(
                                        event,
                                        handled,
                                        error_text,
                                        repeat_count,
                                    )
                                    await send_ops_webhook_log(title, desc, color=color, fields=fields)
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
