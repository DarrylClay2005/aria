from __future__ import annotations

import asyncio
import os
import re
import time
from typing import Any

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional in lightweight local test shells
    aiomysql = None

from core.database import db
from core.override import override_manager


DRONE_NAMES = ("gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream", "alucard", "sapphire", "strife", "lockhart")
DRONE_SCHEMA_OVERRIDES = {
    "strife": os.getenv("ARIA_STRIFE_DB_SCHEMA", "discord_music") or "discord_music",
    "lockhart": os.getenv("ARIA_LOCKHART_DB_SCHEMA", "discord_music") or "discord_music",
}


def schema_for_drone(bot_name: str) -> str:
    cleaned = str(bot_name or "").strip().lower()
    if cleaned not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    return DRONE_SCHEMA_OVERRIDES.get(cleaned, f"discord_music_{cleaned}")
FILTER_ALIASES = {
    "none": "none",
    "unfiltered": "none",
    "highquality": "none",
    "high_quality": "none",
    "hq": "none",
    "nightcore": "nightcore",
    "vaporwave": "vaporwave",
    "bassboost": "bassboost",
    "8d": "8d",
    "8daudio": "8d",
    "karaoke": "karaoke",
    "tremolo": "tremolo",
    "vibrato": "vibrato",
    "lowpass": "lowpass",
    "low_pass": "lowpass",
    "lofi": "lofi",
    "lo-fi": "lofi",
    "electronic": "electronic",
    "party": "party",
    "radio": "radio",
    "cinema": "cinema",
}
LOOP_ALIASES = {"off": "off", "song": "song", "queue": "queue"}
CHANNEL_MENTION_RE = re.compile(r"<#(\d+)>")
LAVALINK_QUERY_PREFIX_RE = re.compile(r"^[a-z0-9_]+search\d*:", re.IGNORECASE)
GUILD_SETTINGS_COLUMNS = (
    ("home_vc_id", "BIGINT"),
    ("volume", "INT DEFAULT 100"),
    ("loop_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("filter_mode", "VARCHAR(20) DEFAULT 'none'"),
    ("dj_role_id", "BIGINT DEFAULT NULL"),
    ("feedback_channel_id", "BIGINT DEFAULT NULL"),
    ("transition_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("fade_seconds", "FLOAT DEFAULT 5.0"),
    ("fade_curve", "VARCHAR(20) DEFAULT 'linear'"),
    ("custom_speed", "FLOAT DEFAULT 1.0"),
    ("custom_pitch", "FLOAT DEFAULT 1.0"),
    ("custom_modifiers_left", "INT DEFAULT 0"),
    ("dj_only_mode", "BOOLEAN DEFAULT FALSE"),
    ("stay_in_vc", "BOOLEAN DEFAULT FALSE"),
)
SCHEMA_CACHE_RECHECK_SECONDS = max(30.0, float(os.getenv("ARIA_SWARM_SCHEMA_RECHECK_SECONDS", "300") or "300"))
ACTIVE_DRONE_CACHE_TTL_SECONDS = max(1.0, float(os.getenv("ARIA_ACTIVE_DRONE_CACHE_TTL_SECONDS", "5") or "5"))
HOME_CHANNEL_CACHE_TTL_SECONDS = max(1.0, float(os.getenv("ARIA_HOME_CHANNEL_CACHE_TTL_SECONDS", "15") or "15"))
MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS = max(2.0, float(os.getenv("ARIA_MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS", "30") or "30"))
SMART_TITLE_NOISE_RE = re.compile(r"\s*[\[(][^\])]*(?:official|lyrics?|audio|video|visualizer|remaster|sped up|slowed)[^\])]*[\])]\s*", re.IGNORECASE)

_direct_order_schema_ready: set[str] = set()
_direct_order_schema_retry_after: dict[str, float] = {}
_direct_order_schema_locks: dict[str, asyncio.Lock] = {}
_guild_settings_schema_ready: set[str] = set()
_guild_settings_schema_retry_after: dict[str, float] = {}
_guild_settings_schema_locks: dict[str, asyncio.Lock] = {}
_music_intelligence_schema_ready: set[str] = set()
_music_intelligence_schema_retry_after: dict[str, float] = {}
_music_intelligence_schema_locks: dict[str, asyncio.Lock] = {}
_active_drones_cache: dict[int, tuple[float, list[str]]] = {}
_home_channel_cache: dict[tuple[int, str], tuple[float, int | None]] = {}
_music_intelligence_cache: dict[tuple[int, str | None], tuple[float, dict[str, Any]]] = {}


def _schema_lock(locks: dict[str, asyncio.Lock], bot_name: str) -> asyncio.Lock:
    lock = locks.get(bot_name)
    if lock is None:
        lock = asyncio.Lock()
        locks[bot_name] = lock
    return lock


def invalidate_swarm_route_cache(guild_id: int | None = None) -> None:
    if guild_id is None:
        _active_drones_cache.clear()
        _home_channel_cache.clear()
        _music_intelligence_cache.clear()
        return
    guild_key = int(guild_id)
    _active_drones_cache.pop(guild_key, None)
    for cache_key in [key for key in _home_channel_cache if key[0] == guild_key]:
        _home_channel_cache.pop(cache_key, None)
    for cache_key in [key for key in _music_intelligence_cache if key[0] == guild_key]:
        _music_intelligence_cache.pop(cache_key, None)


def normalize_drone_name(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = name.strip().lower()
    return cleaned if cleaned in DRONE_NAMES else None


def extract_drone_name(text: str) -> str | None:
    lowered = (text or "").lower()
    for drone in DRONE_NAMES:
        if re.search(rf"\b{re.escape(drone)}\b", lowered):
            return drone
    return None


def extract_channel_id(text: str) -> int | None:
    if not text:
        return None
    match = CHANNEL_MENTION_RE.search(text)
    if match:
        return int(match.group(1))
    bare_digits = re.search(r"\b(\d{15,22})\b", text)
    return int(bare_digits.group(1)) if bare_digits else None


def smart_query_from_title(title: str | None) -> str:
    cleaned = re.sub(r"https?://\S+", "", str(title or ""))
    cleaned = SMART_TITLE_NOISE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|")
    return cleaned[:180] or str(title or "").strip()[:180]


def resolve_bot(ctx):
    bot = getattr(ctx, "bot", None) or getattr(ctx, "client", None)
    if bot is not None:
        return bot
    state = getattr(ctx, "_state", None)
    getter = getattr(state, "_get_client", None)
    if callable(getter):
        return getter()
    return None


def actor_from_ctx(ctx):
    return getattr(ctx, "author", None) or getattr(ctx, "user", None)


def guild_from_ctx(ctx):
    return getattr(ctx, "guild", None)


def guild_id_from_ctx(ctx) -> int | None:
    guild = guild_from_ctx(ctx)
    return guild.id if guild else getattr(ctx, "guild_id", None)


def channel_id_from_ctx(ctx) -> int | None:
    channel = getattr(ctx, "channel", None)
    return channel.id if channel else getattr(ctx, "channel_id", None)


def voice_channel_id_from_ctx(ctx) -> int | None:
    actor = actor_from_ctx(ctx)
    if actor and getattr(actor, "voice", None) and actor.voice.channel:
        return actor.voice.channel.id
    return None


def is_admin_or_override(ctx) -> bool:
    actor = actor_from_ctx(ctx)
    if actor is None:
        return False
    if override_manager.can_override(actor.id):
        return True
    perms = getattr(actor, "guild_permissions", None)
    return bool(perms and perms.administrator)


async def ensure_direct_order_schema(cur, bot_name: str) -> None:
    """Keep Aria's direct-order table compatible with every music bot build."""
    if bot_name not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    if bot_name in _direct_order_schema_ready:
        return
    now = time.monotonic()
    if _direct_order_schema_retry_after.get(bot_name, 0.0) > now:
        return
    async with _schema_lock(_direct_order_schema_locks, bot_name):
        if bot_name in _direct_order_schema_ready:
            return
        try:
            await _ensure_direct_order_schema_uncached(cur, bot_name)
        except Exception:
            _direct_order_schema_retry_after[bot_name] = time.monotonic() + SCHEMA_CACHE_RECHECK_SECONDS
            raise
        _direct_order_schema_ready.add(bot_name)
        _direct_order_schema_retry_after.pop(bot_name, None)


async def _ensure_direct_order_schema_uncached(cur, bot_name: str) -> None:
    await cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            bot_name VARCHAR(50) NOT NULL,
            guild_id BIGINT NOT NULL,
            vc_id BIGINT NULL,
            text_channel_id BIGINT NULL,
            command VARCHAR(50) NOT NULL,
            data TEXT NULL,
            attempts INT NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            claimed_at TIMESTAMP NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_unclaimed (bot_name, guild_id, claimed_at, id),
            INDEX idx_recent_command (bot_name, guild_id, command, created_at)
        )
        """
    )
    for stmt in (
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN vc_id BIGINT NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN text_channel_id BIGINT NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN command VARCHAR(50) NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN data TEXT NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN attempts INT NOT NULL DEFAULT 0",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN last_error TEXT NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN claimed_at TIMESTAMP NULL",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD INDEX idx_unclaimed (bot_name, guild_id, claimed_at, id)",
        f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders ADD INDEX idx_recent_command (bot_name, guild_id, command, created_at)",
    ):
        try:
            await cur.execute(stmt)
        except Exception:
            pass


async def insert_direct_order(cur, bot_name: str, guild_id: int, vc_id: int | None, text_channel_id: int | None, command: str, data: str | None = None, *, dedupe: bool = True) -> None:
    command = str(command or "").upper().strip()
    await ensure_direct_order_schema(cur, bot_name)
    if dedupe:
        await cur.execute(
            f"DELETE FROM {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders WHERE bot_name = %s AND guild_id = %s AND command = %s",
            (bot_name, int(guild_id or 0), command),
        )
    await cur.execute(
        f"INSERT INTO {schema_for_drone(bot_name)}.{bot_name}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
        (bot_name, int(guild_id or 0), vc_id if vc_id else None, text_channel_id if text_channel_id else None, command, data or ""),
    )
    invalidate_swarm_route_cache(guild_id)


async def ensure_guild_settings_schema(cur, bot_name: str) -> None:
    if bot_name not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    if bot_name in _guild_settings_schema_ready:
        return
    now = time.monotonic()
    if _guild_settings_schema_retry_after.get(bot_name, 0.0) > now:
        return
    async with _schema_lock(_guild_settings_schema_locks, bot_name):
        if bot_name in _guild_settings_schema_ready:
            return
        try:
            await _ensure_guild_settings_schema_uncached(cur, bot_name)
        except Exception:
            _guild_settings_schema_retry_after[bot_name] = time.monotonic() + SCHEMA_CACHE_RECHECK_SECONDS
            raise
        _guild_settings_schema_ready.add(bot_name)
        _guild_settings_schema_retry_after.pop(bot_name, None)


async def _ensure_guild_settings_schema_uncached(cur, bot_name: str) -> None:
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema_for_drone(bot_name)}.{bot_name}_guild_settings "
        "(guild_id BIGINT PRIMARY KEY)"
    )
    for column_name, definition in GUILD_SETTINGS_COLUMNS:
        try:
            await cur.execute(
                f"ALTER TABLE {schema_for_drone(bot_name)}.{bot_name}_guild_settings "
                f"ADD COLUMN {column_name} {definition}"
            )
        except Exception:
            pass


async def ensure_music_intelligence_schema(cur, bot_name: str) -> None:
    if bot_name not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    if bot_name in _music_intelligence_schema_ready:
        return
    now = time.monotonic()
    if _music_intelligence_schema_retry_after.get(bot_name, 0.0) > now:
        return
    async with _schema_lock(_music_intelligence_schema_locks, bot_name):
        if bot_name in _music_intelligence_schema_ready:
            return
        try:
            await _ensure_music_intelligence_schema_uncached(cur, bot_name)
        except Exception:
            _music_intelligence_schema_retry_after[bot_name] = time.monotonic() + SCHEMA_CACHE_RECHECK_SECONDS
            raise
        _music_intelligence_schema_ready.add(bot_name)
        _music_intelligence_schema_retry_after.pop(bot_name, None)


async def _ensure_music_intelligence_schema_uncached(cur, bot_name: str) -> None:
    schema = schema_for_drone(bot_name)
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.{bot_name}_track_intelligence ("
        "guild_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, "
        "queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, "
        "skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, "
        "total_listen_seconds INT NOT NULL DEFAULT 0, last_requester_id BIGINT DEFAULT NULL, source VARCHAR(40) DEFAULT 'unknown', "
        "first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_queued TIMESTAMP NULL DEFAULT NULL, last_played TIMESTAMP NULL DEFAULT NULL, "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, url_key))"
    )
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.{bot_name}_user_track_affinity ("
        "guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, "
        "queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, "
        "skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, "
        "score FLOAT NOT NULL DEFAULT 0, last_requested TIMESTAMP NULL DEFAULT NULL, "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, user_id, url_key))"
    )
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.{bot_name}_smart_recommendations ("
        "id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT NOT NULL, requester_id BIGINT DEFAULT NULL, "
        "seed_title TEXT, seed_url TEXT, query_text TEXT, chosen_url TEXT, chosen_title TEXT, "
        "reason VARCHAR(80), accepted BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    for stmt in (
        f"CREATE INDEX {bot_name}_track_intelligence_recent_idx ON {schema}.{bot_name}_track_intelligence (guild_id, last_played)",
        f"CREATE INDEX {bot_name}_track_intelligence_requester_idx ON {schema}.{bot_name}_track_intelligence (guild_id, last_requester_id, last_played)",
        f"CREATE INDEX {bot_name}_user_affinity_recent_idx ON {schema}.{bot_name}_user_track_affinity (guild_id, user_id, last_requested)",
        f"CREATE INDEX {bot_name}_smart_recommendations_recent_idx ON {schema}.{bot_name}_smart_recommendations (guild_id, created_at)",
    ):
        try:
            await cur.execute(stmt)
        except Exception:
            pass


class SwarmController:
    @staticmethod
    def _dict_cursor():
        return aiomysql.DictCursor if aiomysql else None

    async def _guild_targets(self, guild_id: int, *, preferred: str | None = None, active_only: bool = False) -> list[str]:
        if preferred:
            return [preferred]

        active = await self.active_drones(guild_id)
        if active:
            return active

        return [] if active_only else list(DRONE_NAMES)

    async def active_drones(self, guild_id: int) -> list[str]:
        active = []
        if not db.pool:
            return active
        guild_key = int(guild_id or 0)
        now = time.monotonic()
        cached = _active_drones_cache.get(guild_key)
        if cached and cached[0] > now:
            return list(cached[1])

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for drone in DRONE_NAMES:
                    try:
                        # FIX: only consider a drone "active" if it is actually playing,
                        # not merely paused or stopped with a stale playback_state row.
                        await cur.execute(
                            f"SELECT guild_id FROM {schema_for_drone(drone)}.{drone}_playback_state"
                            f" WHERE guild_id = %s AND is_playing = TRUE LIMIT 1",
                            (guild_id,),
                        )
                        row = await cur.fetchone()
                    except Exception:
                        row = None
                    if row:
                        active.append(drone)
        _active_drones_cache[guild_key] = (time.monotonic() + ACTIVE_DRONE_CACHE_TTL_SECONDS, list(active))
        return active

    async def resolve_play_target(self, guild_id: int, requested_drone: str | None, requested_vc_id: int | None) -> tuple[str, int | None]:
        drone = normalize_drone_name(requested_drone)
        if drone:
            return drone, requested_vc_id

        if not db.pool:
            return "gws", requested_vc_id

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for candidate in DRONE_NAMES:
                    try:
                        await cur.execute(
                            f"SELECT channel_id FROM {schema_for_drone(candidate)}.{candidate}_playback_state WHERE guild_id = %s AND is_playing = TRUE LIMIT 1",
                            (guild_id,),
                        )
                        row = await cur.fetchone()
                    except Exception:
                        row = None
                    if row:
                        return candidate, row.get("channel_id") or requested_vc_id

                if requested_vc_id:
                    for candidate in DRONE_NAMES:
                        try:
                            await cur.execute(
                                f"SELECT home_vc_id FROM {schema_for_drone(candidate)}.{candidate}_bot_home_channels WHERE guild_id = %s LIMIT 1",
                                (guild_id,),
                            )
                            row = await cur.fetchone()
                        except Exception:
                            row = None
                        if row and row.get("home_vc_id") == requested_vc_id:
                            return candidate, requested_vc_id

        return "gws", requested_vc_id

    @staticmethod
    def normalize_query(query: str) -> str:
        cleaned = (query or "").strip()
        if not cleaned:
            return ""
        if cleaned.startswith(("http://", "https://")) or LAVALINK_QUERY_PREFIX_RE.match(cleaned):
            return cleaned
        return f"ytmsearch:{cleaned}"

    async def play(self, ctx, query: str, *, drone: str | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "I can't route swarm audio outside a server."

        target_drone, target_vc_id = await self.resolve_play_target(guild_id, drone, voice_channel_id_from_ctx(ctx))
        if not target_vc_id:
            return "Join a voice channel first or set a home channel for the target bot."

        payload = self.normalize_query(query)
        if not payload:
            return "Tell me what to play before I waste a bot on nothing."

        return await self.direct(ctx, target_drone, "PLAY", payload, target_vc_id=target_vc_id)

    async def override(self, ctx, command: str, *, drone: str | None = None) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to push swarm overrides through me."

        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "Swarm overrides only make sense inside a server."

        # FIX: guard against uninitialized pool — same pattern as direct()
        if not db.pool:
            return "My swarm database link is offline right now."

        # FIX: use active_only=False so commands like "pause gws" still route even
        # when that bot is idle/paused (the music bot will no-op if it can't execute).
        # This also prevents the broken active_only path from silently eating commands.
        normalized_drone = normalize_drone_name(drone)
        targets = await self._guild_targets(guild_id, preferred=normalized_drone, active_only=False)
        # _guild_targets never returns [] when active_only=False, but guard anyway.
        if not targets:
            targets = [normalized_drone] if normalized_drone else list(DRONE_NAMES)

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for bot_name in targets:
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {schema_for_drone(bot_name)}.{bot_name}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO {schema_for_drone(bot_name)}.{bot_name}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)",
                        (guild_id, bot_name, command),
                    )

        bot_label = f"`{targets[0]}`" if len(targets) == 1 else f"{len(targets)} nodes"
        return f"Swarm override `{command}` pushed to {bot_label}."

    async def direct(self, ctx, drone: str, action: str, data: str = "", *, target_vc_id: int | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "Direct swarm orders only work inside a server."
        if not db.pool:
            return "My swarm database link is offline right now."

        bot_name = normalize_drone_name(drone)
        if not bot_name:
            return "I need a valid swarm node name for that order."

        vc_id = target_vc_id or voice_channel_id_from_ctx(ctx)
        if not vc_id and action == "PLAY":
            vc_id = await self._lookup_home_channel(guild_id, bot_name)
        if not vc_id and action == "PLAY":
            return "Join a voice channel first or set a home channel for that node."

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await insert_direct_order(
                    cur,
                    bot_name,
                    guild_id,
                    vc_id if vc_id else None,
                    channel_id_from_ctx(ctx),
                    action,
                    data or "",
                )

        if action == "PLAY":
            return f"Queued `{data}` through `{bot_name}`."
        return f"Injected `{action}` directly into `{bot_name}`."

    async def leave(self, ctx, *, drone: str | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "Direct swarm orders only work inside a server."

        preferred = normalize_drone_name(drone)
        if preferred:
            return await self.direct(ctx, preferred, "LEAVE")

        active = await self.active_drones(guild_id)
        if active:
            return await self.direct(ctx, active[0], "LEAVE")

        requested_vc_id = voice_channel_id_from_ctx(ctx)
        if requested_vc_id and db.pool:
            for candidate in DRONE_NAMES:
                home_channel_id = await self._lookup_home_channel(guild_id, candidate)
                if home_channel_id == requested_vc_id:
                    return await self.direct(ctx, candidate, "LEAVE")

        return "I couldn't identify which swarm node should leave. Name the node explicitly."

    async def broadcast(self, ctx, query: str) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to broadcast orders across the swarm."

        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "Broadcast only works inside a server."

        # FIX: guard against uninitialized pool
        if not db.pool:
            return "My swarm database link is offline right now."

        payload = self.normalize_query(query)
        deployed = 0

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for bot_name in DRONE_NAMES:
                    try:
                        await cur.execute(
                            f"SELECT home_vc_id FROM {schema_for_drone(bot_name)}.{bot_name}_bot_home_channels WHERE guild_id = %s LIMIT 1",
                            (guild_id,),
                        )
                        row = await cur.fetchone()
                    except Exception:
                        row = None
                    if not row or not row.get("home_vc_id"):
                        continue

                    await insert_direct_order(
                        cur,
                        bot_name,
                        guild_id,
                        row["home_vc_id"],
                        channel_id_from_ctx(ctx),
                        "PLAY",
                        payload,
                        dedupe=False,
                    )
                    deployed += 1

        if deployed == 0:
            return "No swarm nodes have home channels set in this server."
        return f"Broadcast payload deployed to {deployed} swarm nodes."

    async def _lookup_home_channel(self, guild_id: int, drone: str) -> int | None:
        # FIX: guard against uninitialized pool before attempting acquire
        if not db.pool:
            return None
        cache_key = (int(guild_id or 0), drone)
        now = time.monotonic()
        cached = _home_channel_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT home_vc_id FROM {schema_for_drone(drone)}.{drone}_bot_home_channels WHERE guild_id = %s LIMIT 1",
                        (guild_id,),
                    )
                    row = await cur.fetchone()
                except Exception:
                    row = None
        home_channel_id = row.get("home_vc_id") if row else None
        _home_channel_cache[cache_key] = (time.monotonic() + HOME_CHANNEL_CACHE_TTL_SECONDS, home_channel_id)
        return home_channel_id

    async def set_home(self, ctx, drone: str, channel_id: int) -> str:
        if not db.pool:
            return "Database pool is not ready yet; swarm home channels cannot be updated."
        if not is_admin_or_override(ctx):
            return "You do not have clearance to rewrite swarm home channels."

        guild = guild_from_ctx(ctx)
        bot_name = normalize_drone_name(drone)
        if guild is None or bot_name is None:
            return "I need both a valid server and a valid swarm node for that."

        channel = guild.get_channel(channel_id)
        if channel is None:
            return "I couldn't resolve that voice or stage channel."

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {schema_for_drone(bot_name)}.{bot_name}_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))"
                )
                await cur.execute(
                    f"REPLACE INTO {schema_for_drone(bot_name)}.{bot_name}_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)",
                    (guild.id, bot_name, channel.id),
                )
        invalidate_swarm_route_cache(guild.id)
        return f"Set `{bot_name}` home channel to `{channel.name}`."

    async def set_loop(self, ctx, mode: str, *, drone: str | None = None) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to rewrite loop settings."

        mode_name = LOOP_ALIASES.get((mode or "").strip().lower())
        if not mode_name:
            return "Loop mode must be one of: off, song, queue."

        guild_id = guild_id_from_ctx(ctx)
        # FIX: normalize once so we never put None into the target list
        normalized_drone = normalize_drone_name(drone)
        targets = [normalized_drone] if normalized_drone else list(DRONE_NAMES)

        # FIX: guard against uninitialized pool
        if not db.pool:
            return "My swarm database link is offline right now."

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for bot_name in targets:
                    await ensure_guild_settings_schema(cur, bot_name)
                    await cur.execute(
                        f"INSERT INTO {schema_for_drone(bot_name)}.{bot_name}_guild_settings (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s",
                        (guild_id, mode_name, mode_name),
                    )
        target_label = f"`{targets[0]}`" if len(targets) == 1 else "the whole swarm"
        return f"Loop mode set to `{mode_name}` for {target_label}."

    async def queue_view(self, ctx, *, drone: str | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        bot_name = normalize_drone_name(drone)
        if bot_name is None:
            active = await self.active_drones(guild_id)
            bot_name = active[0] if active else "gws"

        # FIX: guard against uninitialized pool
        if not db.pool:
            return "My swarm database link is offline right now."

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT title FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC LIMIT 10",
                        (guild_id, bot_name),
                    )
                    rows = await cur.fetchall()
                except Exception:
                    rows = []

        if not rows:
            return f"`{bot_name}` has nothing queued right now."
        return "\n".join([f"{index}. {row['title']}" for index, row in enumerate(rows, 1)])

    async def _music_intelligence_snapshot(self, guild_id: int, *, drone: str | None = None) -> dict[str, Any]:
        cache_key = (int(guild_id or 0), normalize_drone_name(drone))
        now = time.monotonic()
        cached = _music_intelligence_cache.get(cache_key)
        if cached and cached[0] > now:
            return dict(cached[1])

        targets = [cache_key[1]] if cache_key[1] else list(DRONE_NAMES)
        bots: list[dict[str, Any]] = []
        totals = {
            "learned_tracks": 0,
            "plays": 0,
            "finishes": 0,
            "skips": 0,
            "likes": 0,
            "dislikes": 0,
            "recommendations": 0,
        }
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for bot_name in targets:
                    try:
                        await ensure_music_intelligence_schema(cur, bot_name)
                        await cur.execute(
                            f"""
                            SELECT COUNT(*) AS learned_tracks,
                                   COALESCE(SUM(play_count), 0) AS plays,
                                   COALESCE(SUM(finish_count), 0) AS finishes,
                                   COALESCE(SUM(skip_count), 0) AS skips,
                                   COALESCE(SUM(like_count), 0) AS likes,
                                   COALESCE(SUM(dislike_count), 0) AS dislikes
                            FROM {schema_for_drone(bot_name)}.{bot_name}_track_intelligence
                            WHERE guild_id = %s
                            """,
                            (guild_id,),
                        )
                        summary = await cur.fetchone() or {}
                        await cur.execute(
                            f"""
                            SELECT title, video_url, play_count, finish_count, skip_count, like_count, dislike_count,
                                   ((finish_count * 3) + (like_count * 5) + play_count - (skip_count * 2) - (dislike_count * 5)) AS smart_score
                            FROM {schema_for_drone(bot_name)}.{bot_name}_track_intelligence
                            WHERE guild_id = %s
                            ORDER BY smart_score DESC, updated_at DESC
                            LIMIT 3
                            """,
                            (guild_id,),
                        )
                        top_tracks = await cur.fetchall() or []
                        await cur.execute(
                            f"""
                            SELECT user_id, COUNT(*) AS track_count, COALESCE(SUM(score), 0) AS taste_score,
                                   COALESCE(SUM(like_count), 0) AS likes, COALESCE(SUM(dislike_count), 0) AS dislikes
                            FROM {schema_for_drone(bot_name)}.{bot_name}_user_track_affinity
                            WHERE guild_id = %s
                            GROUP BY user_id
                            ORDER BY taste_score DESC, likes DESC
                            LIMIT 3
                            """,
                            (guild_id,),
                        )
                        top_users = await cur.fetchall() or []
                        await cur.execute(
                            f"SELECT COUNT(*) AS recommendation_count, MAX(created_at) AS last_recommended FROM {schema_for_drone(bot_name)}.{bot_name}_smart_recommendations WHERE guild_id = %s",
                            (guild_id,),
                        )
                        rec_row = await cur.fetchone() or {}
                    except Exception:
                        continue

                    item = {
                        "bot": bot_name,
                        "summary": summary,
                        "top_tracks": list(top_tracks),
                        "top_users": list(top_users),
                        "recommendations": int(rec_row.get("recommendation_count") or 0),
                        "last_recommended": rec_row.get("last_recommended"),
                    }
                    bots.append(item)
                    totals["learned_tracks"] += int(summary.get("learned_tracks") or 0)
                    totals["plays"] += int(summary.get("plays") or 0)
                    totals["finishes"] += int(summary.get("finishes") or 0)
                    totals["skips"] += int(summary.get("skips") or 0)
                    totals["likes"] += int(summary.get("likes") or 0)
                    totals["dislikes"] += int(summary.get("dislikes") or 0)
                    totals["recommendations"] += int(item["recommendations"] or 0)

        snapshot = {"guild_id": int(guild_id), "bots": bots, "totals": totals}
        _music_intelligence_cache[cache_key] = (time.monotonic() + MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS, snapshot)
        return snapshot

    async def music_intelligence(self, ctx, *, drone: str | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "I need a server to inspect swarm music intelligence."
        if not db.pool:
            return "My swarm database link is offline right now."

        snapshot = await self._music_intelligence_snapshot(int(guild_id), drone=drone)
        if not snapshot["bots"]:
            return "I can see the smart music tables, but there is not enough learned taste data here yet."

        totals = snapshot["totals"]
        lines = [
            f"Smart music memory: {totals['learned_tracks']} learned tracks, {totals['likes']} likes, {totals['dislikes']} dislikes, {totals['recommendations']} recommendations."
        ]
        for bot in snapshot["bots"][:5]:
            summary = bot["summary"]
            top = bot["top_tracks"][0] if bot["top_tracks"] else None
            top_title = str(top.get("title") or "no favorite yet")[:90] if top else "no favorite yet"
            lines.append(
                f"`{bot['bot']}`: {int(summary.get('learned_tracks') or 0)} tracks, "
                f"{int(summary.get('plays') or 0)} plays, top seed `{top_title}`."
            )
        return "\n".join(lines)[:1900]

    async def smart_recommend(self, ctx, *, drone: str | None = None) -> str:
        guild_id = guild_id_from_ctx(ctx)
        if not guild_id:
            return "I need a server before I can make a smart recommendation."
        if not db.pool:
            return "My swarm database link is offline right now."

        actor = actor_from_ctx(ctx)
        requester_id = getattr(actor, "id", None)
        target_drone, target_vc_id = await self.resolve_play_target(guild_id, drone, voice_channel_id_from_ctx(ctx))
        if not target_vc_id:
            return "Join a voice channel first or set a home channel for the target bot."

        seed = None
        reason = "server_favorite"
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                await ensure_music_intelligence_schema(cur, target_drone)
                if requester_id:
                    await cur.execute(
                        f"""
                        SELECT title, video_url, score, like_count, dislike_count
                        FROM {schema_for_drone(target_drone)}.{target_drone}_user_track_affinity
                        WHERE guild_id = %s AND user_id = %s AND dislike_count <= like_count
                        ORDER BY score DESC, last_requested DESC
                        LIMIT 1
                        """,
                        (guild_id, requester_id),
                    )
                    seed = await cur.fetchone()
                    if seed:
                        reason = "personal_taste"
                if not seed:
                    await cur.execute(
                        f"""
                        SELECT title, video_url,
                               ((finish_count * 3) + (like_count * 5) + play_count - (skip_count * 2) - (dislike_count * 5)) AS score
                        FROM {schema_for_drone(target_drone)}.{target_drone}_track_intelligence
                        WHERE guild_id = %s AND dislike_count <= like_count
                        ORDER BY score DESC, updated_at DESC
                        LIMIT 1
                        """,
                        (guild_id,),
                    )
                    seed = await cur.fetchone()
                if not seed:
                    return f"`{target_drone}` has smart tables ready, but no usable seeds for this server yet."

                seed_title = str(seed.get("title") or seed.get("video_url") or "").strip()
                query_text = smart_query_from_title(seed_title)
                payload = self.normalize_query(f"{query_text} radio")
                await cur.execute(
                    f"""
                    INSERT INTO {schema_for_drone(target_drone)}.{target_drone}_smart_recommendations
                    (guild_id, requester_id, seed_title, seed_url, query_text, chosen_url, chosen_title, reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        guild_id,
                        requester_id,
                        seed_title,
                        seed.get("video_url"),
                        payload,
                        None,
                        None,
                        reason,
                    ),
                )

        invalidate_swarm_route_cache(guild_id)
        result = await self.direct(ctx, target_drone, "PLAY", payload, target_vc_id=target_vc_id)
        return f"{result}\nSmart seed: `{seed_title[:120]}` ({reason.replace('_', ' ')})."

    async def shuffle(self, ctx, *, drone: str | None = None) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to reshuffle swarm queues."

        guild_id = guild_id_from_ctx(ctx)
        # FIX: normalize once to avoid [None] in target list when drone is invalid/absent
        normalized_drone = normalize_drone_name(drone)
        targets = [normalized_drone] if normalized_drone else list(DRONE_NAMES)
        shuffled = []

        import random

        # FIX: guard against uninitialized pool
        if not db.pool:
            return "My swarm database link is offline right now."

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for bot_name in targets:
                    try:
                        await cur.execute(
                            f"SELECT * FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s",
                            (guild_id, bot_name),
                        )
                        tracks = await cur.fetchall()
                    except Exception:
                        tracks = []
                    if len(tracks) < 2:
                        continue

                    random.shuffle(tracks)
                    await cur.execute(f"DELETE FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s", (guild_id, bot_name))
                    for track in tracks:
                        await cur.execute(
                            f"INSERT INTO {schema_for_drone(bot_name)}.{bot_name}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
                            (track["guild_id"], track["bot_name"], track["video_url"], track["title"], track["requester_id"]),
                        )
                    shuffled.append(bot_name)

        if not shuffled:
            return "There were not enough queued tracks to shuffle."
        return f"Shuffled queues for: {', '.join(shuffled)}."

    async def remove_track(self, ctx, drone: str, track_number: int) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to surgically edit swarm queues."

        guild_id = guild_id_from_ctx(ctx)
        bot_name = normalize_drone_name(drone)
        if not bot_name:
            return "I need a valid swarm node name for that removal."

        if not db.pool:
            return "My swarm database link is offline right now."

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT id, title FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC",
                        (guild_id, bot_name),
                    )
                    rows = await cur.fetchall()
                except Exception:
                    rows = []

                if track_number < 1 or track_number > len(rows):
                    return f"`{bot_name}` only has {len(rows)} tracks queued."

                target = rows[track_number - 1]
                await cur.execute(f"DELETE FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE id = %s AND guild_id = %s AND bot_name = %s", (target["id"], guild_id, bot_name))
        return f"Removed track {track_number} from `{bot_name}`: {target['title']}."

    async def set_filter(self, ctx, filter_type: str, *, drone: str | None = None) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to rewrite swarm audio filters."

        normalized = FILTER_ALIASES.get((filter_type or "").strip().lower().replace(" ", ""))
        if normalized is None:
            return "Filter must be one of: none, nightcore, vaporwave, bassboost, 8d."

        guild_id = guild_id_from_ctx(ctx)
        # FIX: normalize once so we never put None into the target list
        normalized_drone = normalize_drone_name(drone)
        targets = [normalized_drone] if normalized_drone else list(DRONE_NAMES)

        # FIX: guard against uninitialized pool
        if not db.pool:
            return "My swarm database link is offline right now."

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for bot_name in targets:
                    await ensure_guild_settings_schema(cur, bot_name)
                    await cur.execute(
                        f"INSERT INTO {schema_for_drone(bot_name)}.{bot_name}_guild_settings (guild_id, filter_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE filter_mode = %s",
                        (guild_id, normalized, normalized),
                    )
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {schema_for_drone(bot_name)}.{bot_name}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO {schema_for_drone(bot_name)}.{bot_name}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)",
                        (guild_id, bot_name, "UPDATE_FILTER"),
                    )

        target_label = f"`{targets[0]}`" if len(targets) == 1 else "the whole swarm"
        return f"Filter `{normalized}` applied to {target_label}."

    async def radar(self, ctx) -> str:
        if not db.pool:
            return "Database pool is not ready yet; swarm radar is temporarily offline."
        guild_id = guild_id_from_ctx(ctx)
        guild = guild_from_ctx(ctx)
        lines = []

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for bot_name in DRONE_NAMES:
                    try:
                        await cur.execute(
                            f"""
                            SELECT
                                p.guild_id,
                                p.is_playing,
                                (SELECT title FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s ORDER BY id ASC LIMIT 1) AS next_title,
                                (SELECT COUNT(*) FROM {schema_for_drone(bot_name)}.{bot_name}_queue WHERE guild_id = %s) AS q_len
                            FROM {schema_for_drone(bot_name)}.{bot_name}_playback_state p
                            WHERE p.guild_id = %s
                            LIMIT 1
                            """,
                            (guild_id, guild_id, guild_id),
                        )
                        state = await cur.fetchone()
                    except Exception:
                        state = None
                    if not state:
                        continue

                    status = "playing" if state.get("is_playing") else "paused"
                    track = state.get("next_title") or "nothing queued"
                    lines.append(f"{guild.name if guild else guild_id} | {bot_name}: {status}, next up `{track}`, queue {state.get('q_len') or 0}.")

        return "\n".join(lines) if lines else "Grid is quiet. No active swarm nodes reported for this server."

    async def wrapped(self, ctx) -> str:
        if not db.pool:
            return "Database pool is not ready yet; swarm wrapped stats are temporarily offline."
        guild_id = guild_id_from_ctx(ctx)
        counts = {}

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for bot_name in DRONE_NAMES:
                    try:
                        await cur.execute(
                            f"SELECT title FROM {schema_for_drone(bot_name)}.{bot_name}_history WHERE guild_id = %s",
                            (guild_id,),
                        )
                        rows = await cur.fetchall()
                    except Exception:
                        rows = []
                    for row in rows:
                        title = row["title"]
                        counts[title] = counts.get(title, 0) + 1

        if not counts:
            return "Not enough swarm history has been collected yet."

        top_tracks = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:5]
        return "\n".join(f"{index}. {title} ({count} plays)" for index, (title, count) in enumerate(top_tracks, 1))

    async def restore_backup(self, ctx, drone: str) -> str:
        if not is_admin_or_override(ctx):
            return "You do not have clearance to resurrect swarm backups."

        guild_id = guild_id_from_ctx(ctx)
        bot_name = normalize_drone_name(drone)
        if not bot_name:
            return "I need a valid swarm node name to restore a backup."
        if not db.pool:
            return "My swarm database link is offline right now."

        requester = actor_from_ctx(ctx)
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT * FROM {schema_for_drone(bot_name)}.{bot_name}_queue_backup WHERE guild_id = %s ORDER BY id DESC LIMIT 20",
                        (guild_id,),
                    )
                    backups = await cur.fetchall()
                except Exception:
                    backups = []

                if not backups:
                    return f"No recent backups were found for `{bot_name}`."

                for track in reversed(backups):
                    await cur.execute(
                        f"INSERT INTO {schema_for_drone(bot_name)}.{bot_name}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
                        (track["guild_id"], bot_name, track["video_url"], track["title"], requester.id if requester else 0),
                    )

        return f"Restored the latest queue backup for `{bot_name}`."


swarm_controller = SwarmController()
