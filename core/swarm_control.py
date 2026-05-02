from __future__ import annotations

import re

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional in lightweight local test shells
    aiomysql = None

from core.database import db
from core.override import override_manager


DRONE_NAMES = ("gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream", "alucard", "sapphire")
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
}
LOOP_ALIASES = {"off": "off", "song": "song", "queue": "queue"}
CHANNEL_MENTION_RE = re.compile(r"<#(\d+)>")
GUILD_SETTINGS_COLUMNS = (
    ("home_vc_id", "BIGINT"),
    ("volume", "INT DEFAULT 100"),
    ("loop_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("filter_mode", "VARCHAR(20) DEFAULT 'none'"),
    ("dj_role_id", "BIGINT DEFAULT NULL"),
    ("feedback_channel_id", "BIGINT DEFAULT NULL"),
    ("transition_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("custom_speed", "FLOAT DEFAULT 1.0"),
    ("custom_pitch", "FLOAT DEFAULT 1.0"),
    ("custom_modifiers_left", "INT DEFAULT 0"),
    ("dj_only_mode", "BOOLEAN DEFAULT FALSE"),
    ("stay_in_vc", "BOOLEAN DEFAULT FALSE"),
)


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


async def ensure_guild_settings_schema(cur, bot_name: str) -> None:
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_guild_settings "
        "(guild_id BIGINT PRIMARY KEY)"
    )
    for column_name, definition in GUILD_SETTINGS_COLUMNS:
        try:
            await cur.execute(
                f"ALTER TABLE discord_music_{bot_name}.{bot_name}_guild_settings "
                f"ADD COLUMN {column_name} {definition}"
            )
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

        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for drone in DRONE_NAMES:
                    try:
                        # FIX: only consider a drone "active" if it is actually playing,
                        # not merely paused or stopped with a stale playback_state row.
                        await cur.execute(
                            f"SELECT guild_id FROM discord_music_{drone}.{drone}_playback_state"
                            f" WHERE guild_id = %s AND is_playing = TRUE LIMIT 1",
                            (guild_id,),
                        )
                        row = await cur.fetchone()
                    except Exception:
                        row = None
                    if row:
                        active.append(drone)
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
                            f"SELECT channel_id FROM discord_music_{candidate}.{candidate}_playback_state WHERE guild_id = %s AND is_playing = TRUE LIMIT 1",
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
                                f"SELECT home_vc_id FROM discord_music_{candidate}.{candidate}_bot_home_channels WHERE guild_id = %s LIMIT 1",
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
        if cleaned.startswith(("http://", "https://", "ytsearch")):
            return cleaned
        return f"ytsearch1:{cleaned}"

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
                        f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO discord_music_{bot_name}.{bot_name}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)",
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
                await cur.execute(
                    f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)"
                )
                await cur.execute(
                    f"DELETE FROM discord_music_{bot_name}.{bot_name}_swarm_direct_orders WHERE bot_name = %s AND guild_id = %s AND command = %s",
                    (bot_name, guild_id, action),
                )
                await cur.execute(
                    f"INSERT INTO discord_music_{bot_name}.{bot_name}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
                    # FIX: store NULL (not 0) for missing vc_id — the music bots check
                    # truthiness of vc_id, so 0 is treated as "no channel" AND is a
                    # valid-looking value that silently falls through to the home channel
                    # fallback instead of routing correctly.
                    (bot_name, guild_id, vc_id if vc_id else None, channel_id_from_ctx(ctx), action, data or ""),
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
            async with db.pool.acquire() as conn:
                async with conn.cursor(self._dict_cursor()) as cur:
                    for candidate in DRONE_NAMES:
                        try:
                            await cur.execute(
                                f"SELECT home_vc_id FROM discord_music_{candidate}.{candidate}_bot_home_channels "
                                "WHERE guild_id = %s LIMIT 1",
                                (guild_id,),
                            )
                            row = await cur.fetchone()
                        except Exception:
                            row = None
                        if row and row.get("home_vc_id") == requested_vc_id:
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
                            f"SELECT home_vc_id FROM discord_music_{bot_name}.{bot_name}_bot_home_channels WHERE guild_id = %s LIMIT 1",
                            (guild_id,),
                        )
                        row = await cur.fetchone()
                    except Exception:
                        row = None
                    if not row or not row.get("home_vc_id"):
                        continue

                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)"
                    )
                    await cur.execute(
                        f"INSERT INTO discord_music_{bot_name}.{bot_name}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_name, guild_id, row["home_vc_id"], channel_id_from_ctx(ctx), "PLAY", payload),
                    )
                    deployed += 1

        if deployed == 0:
            return "No swarm nodes have home channels set in this server."
        return f"Broadcast payload deployed to {deployed} swarm nodes."

    async def _lookup_home_channel(self, guild_id: int, drone: str) -> int | None:
        # FIX: guard against uninitialized pool before attempting acquire
        if not db.pool:
            return None
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT home_vc_id FROM discord_music_{drone}.{drone}_bot_home_channels WHERE guild_id = %s LIMIT 1",
                        (guild_id,),
                    )
                    row = await cur.fetchone()
                except Exception:
                    row = None
        return row.get("home_vc_id") if row else None

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
                    f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))"
                )
                await cur.execute(
                    f"REPLACE INTO discord_music_{bot_name}.{bot_name}_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)",
                    (guild.id, bot_name, channel.id),
                )
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
                        f"INSERT INTO discord_music_{bot_name}.{bot_name}_guild_settings (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s",
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
                        f"SELECT title FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC LIMIT 10",
                        (guild_id, bot_name),
                    )
                    rows = await cur.fetchall()
                except Exception:
                    rows = []

        if not rows:
            return f"`{bot_name}` has nothing queued right now."
        return "\n".join([f"{index}. {row['title']}" for index, row in enumerate(rows, 1)])

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
                            f"SELECT * FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s",
                            (guild_id, bot_name),
                        )
                        tracks = await cur.fetchall()
                    except Exception:
                        tracks = []
                    if len(tracks) < 2:
                        continue

                    random.shuffle(tracks)
                    await cur.execute(f"DELETE FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s", (guild_id, bot_name))
                    for track in tracks:
                        await cur.execute(
                            f"INSERT INTO discord_music_{bot_name}.{bot_name}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
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
                        f"SELECT id, title FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC",
                        (guild_id, bot_name),
                    )
                    rows = await cur.fetchall()
                except Exception:
                    rows = []

                if track_number < 1 or track_number > len(rows):
                    return f"`{bot_name}` only has {len(rows)} tracks queued."

                target = rows[track_number - 1]
                await cur.execute(f"DELETE FROM discord_music_{bot_name}.{bot_name}_queue WHERE id = %s AND guild_id = %s AND bot_name = %s", (target["id"], guild_id, bot_name))
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
                        f"INSERT INTO discord_music_{bot_name}.{bot_name}_guild_settings (guild_id, filter_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE filter_mode = %s",
                        (guild_id, normalized, normalized),
                    )
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS discord_music_{bot_name}.{bot_name}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO discord_music_{bot_name}.{bot_name}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)",
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
                            f"SELECT guild_id, is_playing FROM discord_music_{bot_name}.{bot_name}_playback_state WHERE guild_id = %s LIMIT 1",
                            (guild_id,),
                        )
                        state = await cur.fetchone()
                    except Exception:
                        state = None
                    if not state:
                        continue

                    try:
                        await cur.execute(
                            f"SELECT title FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s ORDER BY id ASC LIMIT 1",
                            (guild_id,),
                        )
                        track_row = await cur.fetchone()
                    except Exception:
                        track_row = None

                    try:
                        await cur.execute(
                            f"SELECT COUNT(*) AS q_len FROM discord_music_{bot_name}.{bot_name}_queue WHERE guild_id = %s",
                            (guild_id,),
                        )
                        q_len_row = await cur.fetchone()
                    except Exception:
                        q_len_row = {"q_len": 0}

                    status = "playing" if state.get("is_playing") else "paused"
                    track = track_row["title"] if track_row else "nothing queued"
                    lines.append(f"{guild.name if guild else guild_id} | {bot_name}: {status}, next up `{track}`, queue {q_len_row['q_len']}.")

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
                            f"SELECT title FROM discord_music_{bot_name}.{bot_name}_history WHERE guild_id = %s",
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

        requester = actor_from_ctx(ctx)
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute(
                        f"SELECT * FROM discord_music_{bot_name}.{bot_name}_queue_backup WHERE guild_id = %s ORDER BY id DESC LIMIT 20",
                        (guild_id,),
                    )
                    backups = await cur.fetchall()
                except Exception:
                    backups = []

                if not backups:
                    return f"No recent backups were found for `{bot_name}`."

                for track in reversed(backups):
                    await cur.execute(
                        f"INSERT INTO discord_music_{bot_name}.{bot_name}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
                        (track["guild_id"], bot_name, track["video_url"], track["title"], requester.id if requester else 0),
                    )

        return f"Restored the latest queue backup for `{bot_name}`."


swarm_controller = SwarmController()
