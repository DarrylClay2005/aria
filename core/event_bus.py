from __future__ import annotations

import hashlib
import json
import logging
import asyncio
from typing import Any

try:
    import aiomysql
except ImportError:  # pragma: no cover
    aiomysql = None

from core.database import db

logger = logging.getLogger("aria.event_bus")

DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream", "alucard", "sapphire"]
BOT_SCHEMAS = {
    drone: {
        "schema": f"discord_music_{drone}",
        "queue": f"{drone}_queue",
        "backup": f"{drone}_queue_backup",
        "playback": f"{drone}_playback_state",
        "home": f"{drone}_bot_home_channels",
        "errors": f"{drone}_error_events",
    }
    for drone in DRONE_NAMES
}


class EventBus:
    def __init__(self, bot):
        self.bot = bot
        self._last_claimed_id = 0
        self._claim_lock = asyncio.Lock()

    @staticmethod
    def _dict_cursor():
        return aiomysql.DictCursor if aiomysql else None

    async def initialize(self) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_swarm_events (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        event_type VARCHAR(64) NOT NULL,
                        source_system VARCHAR(64) NOT NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        severity VARCHAR(16) NOT NULL DEFAULT 'info',
                        payload_json LONGTEXT NULL,
                        dedupe_key VARCHAR(255) NULL,
                        processed BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_dedupe (dedupe_key)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_event_cursors (
                        cursor_key VARCHAR(255) PRIMARY KEY,
                        cursor_value VARCHAR(255) NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_swarm_health (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        bot_name VARCHAR(64) NOT NULL,
                        guild_id BIGINT NULL,
                        state_signature VARCHAR(128) NOT NULL,
                        state_json LONGTEXT NULL,
                        health_score DOUBLE NOT NULL DEFAULT 0,
                        status_label VARCHAR(64) NOT NULL DEFAULT 'unknown',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_bot_guild (bot_name, guild_id)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_swarm_health_history (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        bot_name VARCHAR(64) NOT NULL,
                        guild_id BIGINT NULL,
                        health_score DOUBLE NOT NULL DEFAULT 0,
                        status_label VARCHAR(64) NOT NULL DEFAULT 'unknown',
                        state_signature VARCHAR(128) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_bot_guild_created (bot_name, guild_id, created_at)
                    )
                    """
                )

    async def _get_cursor(self, cur, key: str, default: str = "0") -> str:
        await cur.execute("SELECT cursor_value FROM aria_event_cursors WHERE cursor_key=%s", (key,))
        row = await cur.fetchone()
        if not row:
            return default
        if isinstance(row, dict):
            return str(row.get("cursor_value", default))
        return str(row[0])

    async def _set_cursor(self, cur, key: str, value: str) -> None:
        await cur.execute(
            """
            INSERT INTO aria_event_cursors (cursor_key, cursor_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE cursor_value = VALUES(cursor_value)
            """,
            (key[:255], str(value)[:255]),
        )

    async def _table_columns(self, cur, schema: str, table: str) -> set[str]:
        await cur.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            """,
            (schema, table),
        )
        rows = await cur.fetchall() or []
        cols = set()
        for row in rows:
            if isinstance(row, dict):
                cols.add(str(row.get("COLUMN_NAME") or row.get("column_name") or ""))
            else:
                cols.add(str(row[0]))
        return cols

    async def _playback_sync_query(self, cur, drone: str) -> str:
        cfg = BOT_SCHEMAS[drone]
        cols = await self._table_columns(cur, cfg["schema"], cfg["playback"])
        queue_cols = await self._table_columns(cur, cfg["schema"], cfg["queue"])
        backup_cols = await self._table_columns(cur, cfg["schema"], cfg["backup"])
        home_cols = await self._table_columns(cur, cfg["schema"], cfg["home"])
        channel_expr = "p.channel_id" if "channel_id" in cols else "NULL"
        text_expr = "p.text_channel_id" if "text_channel_id" in cols else "NULL"
        pos_expr = "p.position_seconds" if "position_seconds" in cols else "0"
        play_expr = "p.is_playing" if "is_playing" in cols else "FALSE"
        if "current_track" in cols and "video_url" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(p.current_track, ''), NULLIF(p.video_url, ''), NULLIF(p.title, ''))"
        elif "current_track" in cols and "video_url" in cols:
            track_expr = "COALESCE(NULLIF(p.current_track, ''), NULLIF(p.video_url, ''))"
        elif "current_track" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(p.current_track, ''), NULLIF(p.title, ''))"
        elif "video_url" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(p.video_url, ''), NULLIF(p.title, ''))"
        elif "video_url" in cols:
            track_expr = "NULLIF(p.video_url, '')"
        elif "title" in cols:
            track_expr = "NULLIF(p.title, '')"
        elif "current_track" in cols:
            track_expr = "NULLIF(p.current_track, '')"
        else:
            track_expr = "NULL"
        updated_expr = "TIMESTAMPDIFF(SECOND, p.updated_at, NOW())" if "updated_at" in cols else "NULL"
        playback_filter = f"WHERE (p.bot_name IS NULL OR p.bot_name = '{drone}')" if "bot_name" in cols else ""
        queue_filter = f" AND (q.bot_name IS NULL OR q.bot_name = '{drone}')" if "bot_name" in queue_cols else ""
        backup_filter = f" AND (b.bot_name IS NULL OR b.bot_name = '{drone}')" if "bot_name" in backup_cols else ""
        home_join = f"h.guild_id = p.guild_id AND (h.bot_name IS NULL OR h.bot_name = '{drone}')" if "bot_name" in home_cols else "h.guild_id = p.guild_id"
        return f"""
                SELECT p.guild_id, {channel_expr} AS channel_id, {text_expr} AS text_channel_id,
                       {track_expr} AS current_track, {pos_expr} AS position_seconds, {play_expr} AS is_playing,
                       {updated_expr} AS updated_seconds,
                       h.home_vc_id,
                       (SELECT COUNT(*) FROM {cfg['schema']}.{cfg['queue']} q WHERE q.guild_id = p.guild_id{queue_filter}) AS queue_count,
                       (SELECT COUNT(*) FROM {cfg['schema']}.{cfg['backup']} b WHERE b.guild_id = p.guild_id{backup_filter}) AS backup_count
                FROM {cfg['schema']}.{cfg['playback']} p
                LEFT JOIN {cfg['schema']}.{cfg['home']} h ON {home_join}
                {playback_filter}
                """

    async def emit_event(
        self,
        *,
        event_type: str,
        source_system: str,
        bot_name: str | None = None,
        guild_id: int | None = None,
        severity: str = "info",
        payload: dict[str, Any] | None = None,
        dedupe_key: str | None = None,
    ) -> None:
        if not db.pool:
            return
        payload_json = json.dumps(payload or {}, default=str)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_swarm_events (event_type, source_system, bot_name, guild_id, severity, payload_json, dedupe_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE created_at = created_at
                    """,
                    (event_type[:64], source_system[:64], bot_name[:64] if bot_name else None, guild_id, severity[:16], payload_json, dedupe_key[:255] if dedupe_key else None),
                )

    def _compute_health(self, state: dict[str, Any]) -> tuple[float, str]:
        score = 0.25
        if state.get("home_vc_id"):
            score += 0.15
        if state.get("queue_count", 0) > 0:
            score += 0.15
        if state.get("backup_count", 0) > 0:
            score += 0.10
        if state.get("current_track"):
            score += 0.15
        if state.get("is_playing"):
            score += 0.15
        if state.get("updated_seconds") is not None:
            stale = float(state.get("updated_seconds") or 0)
            if stale <= 20:
                score += 0.05
            elif stale <= 60:
                score += 0.02
            else:
                score -= 0.08
        if state.get("is_playing") and not state.get("home_vc_id") and not state.get("channel_id"):
            score -= 0.22
        if not state.get("queue_count") and state.get("backup_count", 0) > 0:
            score -= 0.08
        score = max(0.0, min(1.0, score))
        if score >= 0.82:
            label = "healthy"
        elif score >= 0.58:
            label = "recoverable"
        elif score >= 0.35:
            label = "degraded"
        else:
            label = "critical"
        return score, label

    def _signature(self, state: dict[str, Any]) -> str:
        raw = json.dumps(state, sort_keys=True, default=str)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:40]

    async def _sync_bot_state(self, cur, drone: str) -> None:
        try:
            query = await self._playback_sync_query(cur, drone)
            await cur.execute(query)
            rows = await cur.fetchall() or []
        except Exception:
            logger.exception("Failed syncing state for %s", drone)
            return

        for row in rows:
            guild_id = int(row.get("guild_id") or 0)
            state = {
                "drone": drone,
                "guild_id": guild_id,
                "channel_id": row.get("channel_id"),
                "text_channel_id": row.get("text_channel_id"),
                "current_track": bool(row.get("current_track")),
                "position_seconds": float(row.get("position_seconds") or 0),
                "is_playing": bool(row.get("is_playing")),
                "updated_seconds": float(row.get("updated_seconds") or 0),
                "home_vc_id": row.get("home_vc_id"),
                "queue_count": int(row.get("queue_count") or 0),
                "backup_count": int(row.get("backup_count") or 0),
            }
            signature = self._signature(state)
            cursor_key = f"state:{drone}:{guild_id}"
            previous_signature = await self._get_cursor(cur, cursor_key, default="")
            score, label = self._compute_health(state)
            await cur.execute(
                """
                INSERT INTO aria_swarm_health (bot_name, guild_id, state_signature, state_json, health_score, status_label)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE state_signature=VALUES(state_signature), state_json=VALUES(state_json), health_score=VALUES(health_score), status_label=VALUES(status_label)
                """,
                (drone, guild_id, signature, json.dumps(state, default=str), score, label),
            )
            if signature != previous_signature:
                await cur.execute(
                    """
                    INSERT INTO aria_swarm_health_history (bot_name, guild_id, health_score, status_label, state_signature)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (drone, guild_id, score, label, signature),
                )
            await cur.execute(
                """
                SELECT health_score FROM aria_swarm_health_history
                WHERE bot_name=%s AND guild_id=%s
                ORDER BY id DESC LIMIT 4
                """,
                (drone, guild_id),
            )
            recent_scores = await cur.fetchall() or []
            if len(recent_scores) >= 3:
                vals = []
                for item in recent_scores:
                    vals.append(float(item.get("health_score") if isinstance(item, dict) else item[0]))
                if vals and max(vals) - min(vals) >= 0.22 and vals[0] < vals[-1]:
                    await self.emit_event(
                        event_type="health_trending_down",
                        source_system="swarm_health",
                        bot_name=drone,
                        guild_id=guild_id,
                        severity="warning",
                        payload={**state, "health_score": score, "status_label": label, "recent_health": vals[:4]},
                        dedupe_key=f"healthtrend:{drone}:{guild_id}:{signature}",
                    )
            if signature != previous_signature:
                await self.emit_event(
                    event_type="bot_state_changed",
                    source_system="swarm_state",
                    bot_name=drone,
                    guild_id=guild_id,
                    severity="info",
                    payload={**state, "health_score": score, "status_label": label},
                    dedupe_key=f"evt:{drone}:{guild_id}:{signature}",
                )
                if state["queue_count"] > 0 or state["backup_count"] > 0 or state["current_track"]:
                    if not state["is_playing"]:
                        await self.emit_event(
                            event_type="recoverable_state_detected",
                            source_system="swarm_state",
                            bot_name=drone,
                            guild_id=guild_id,
                            severity="warning",
                            payload={**state, "health_score": score, "status_label": label},
                            dedupe_key=f"recoverable:{drone}:{guild_id}:{signature}",
                        )
                    elif state["is_playing"] and not state["home_vc_id"] and not state["channel_id"]:
                        await self.emit_event(
                            event_type="playback_state_drift",
                            source_system="swarm_state",
                            bot_name=drone,
                            guild_id=guild_id,
                            severity="warning",
                            payload={**state, "health_score": score, "status_label": label},
                            dedupe_key=f"drift:{drone}:{guild_id}:{signature}",
                        )
                await self._set_cursor(cur, cursor_key, signature)

    async def _sync_bot_errors(self, cur, drone: str) -> None:
        cfg = BOT_SCHEMAS[drone]
        cursor_key = f"errors:{drone}:last_id"
        last_id = int(await self._get_cursor(cur, cursor_key, default="0"))
        try:
            await cur.execute(
                f"SELECT id, guild_id, error_type, description, created_at FROM {cfg['schema']}.{cfg['errors']} WHERE id > %s ORDER BY id ASC LIMIT 100",
                (last_id,),
            )
            rows = await cur.fetchall() or []
        except Exception:
            return
        max_seen = last_id
        for row in rows:
            event_id = int(row.get("id") or 0)
            max_seen = max(max_seen, event_id)
            await self.emit_event(
                event_type="bot_error_logged",
                source_system="bot_error_table",
                bot_name=drone,
                guild_id=int(row.get("guild_id") or 0) or None,
                severity="error",
                payload={
                    "error_type": row.get("error_type"),
                    "error_message": row.get("description"),
                    "created_at": str(row.get("created_at")),
                },
                dedupe_key=f"boterror:{drone}:{event_id}",
            )
        if max_seen != last_id:
            await self._set_cursor(cur, cursor_key, str(max_seen))

    async def sync_swarm_sources(self) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for drone in DRONE_NAMES:
                    await self._sync_bot_state(cur, drone)
                    await self._sync_bot_errors(cur, drone)

    async def claim_events(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if not db.pool:
            return []
        async with self._claim_lock:
            async with db.pool.acquire() as conn:
                async with conn.cursor(self._dict_cursor()) as cur:
                    await cur.execute(
                        "SELECT id, event_type, source_system, bot_name, guild_id, severity, payload_json, created_at "
                        "FROM aria_swarm_events WHERE processed = FALSE ORDER BY id ASC LIMIT %s",
                        (limit,),
                    )
                    rows = await cur.fetchall() or []
                    events: list[dict[str, Any]] = []
                    max_claimed = self._last_claimed_id
                    for row in rows:
                        payload = {}
                        try:
                            payload = json.loads(row.get("payload_json") or "{}")
                        except Exception:
                            payload = {}
                        event = {
                            "id": int(row.get("id") or 0),
                            "event_type": row.get("event_type"),
                            "source_system": row.get("source_system"),
                            "bot_name": row.get("bot_name"),
                            "guild_id": row.get("guild_id"),
                            "severity": row.get("severity"),
                            "payload": payload,
                            "created_at": str(row.get("created_at")),
                        }
                        events.append(event)
                        max_claimed = max(max_claimed, event["id"])
                    self._last_claimed_id = max_claimed
                    return events

    async def mark_processed(self, event_id: int) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE aria_swarm_events SET processed = TRUE WHERE id = %s", (event_id,))

    async def recent_health_summary(self, *, limit: int = 12) -> list[dict[str, Any]]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    "SELECT bot_name, guild_id, health_score, status_label, state_json, updated_at FROM aria_swarm_health ORDER BY health_score ASC, updated_at DESC LIMIT %s",
                    (limit,),
                )
                return await cur.fetchall() or []
