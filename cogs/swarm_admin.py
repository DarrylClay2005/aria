import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiomysql
import logging
import random
from typing import Any

from core.database import db
from core.swarm_control import ensure_guild_settings_schema
from core.webhooks import send_webhook_log, send_error_webhook_log

logger = logging.getLogger("discord")

DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream"]
DRONES = [app_commands.Choice(name=d.capitalize(), value=d) for d in DRONE_NAMES]
VALID_COMMANDS = {"PLAY", "PAUSE", "RESUME", "SKIP", "STOP", "RESTART", "RECOVER", "LEAVE", "UPDATE_FILTER"}


def _db_name(bot_name: str) -> str:
    if bot_name not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    return f"discord_music_{bot_name}"


def _q(bot_name: str, table_suffix: str) -> str:
    if bot_name not in DRONE_NAMES:
        raise ValueError(f"Unknown swarm node: {bot_name}")
    if not table_suffix.replace("_", "").isalnum():
        raise ValueError(f"Unsafe table suffix: {table_suffix}")
    return f"`discord_music_{bot_name}`.`{bot_name}_{table_suffix}`"


async def _table_exists(cur, bot_name: str, table_suffix: str) -> bool:
    await cur.execute("SHOW TABLES IN `%s` LIKE %%s" % _db_name(bot_name), (f"{bot_name}_{table_suffix}",))
    return bool(await cur.fetchone())


async def _columns(cur, bot_name: str, table_suffix: str) -> set[str]:
    if not await _table_exists(cur, bot_name, table_suffix):
        return set()
    await cur.execute(f"SHOW COLUMNS FROM {_q(bot_name, table_suffix)}")
    rows = await cur.fetchall()
    cols: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            cols.add(str(row.get("Field")))
        else:
            cols.add(str(row[0]))
    return cols


async def _ensure_overrides(cur, bot_name: str) -> None:
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {_q(bot_name, 'swarm_overrides')} ("
        "guild_id BIGINT NOT NULL, "
        "bot_name VARCHAR(50) NOT NULL, "
        "command VARCHAR(20) NOT NULL, "
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
        "PRIMARY KEY(guild_id, bot_name))"
    )


async def _push_override(cur, bot_name: str, guild_id: int, command: str) -> None:
    command = command.upper().strip()
    if command not in VALID_COMMANDS:
        raise ValueError(f"Unsupported command: {command}")
    await _ensure_overrides(cur, bot_name)
    await cur.execute(
        f"REPLACE INTO {_q(bot_name, 'swarm_overrides')} (guild_id, bot_name, command) VALUES (%s, %s, %s)",
        (int(guild_id or 0), bot_name, command),
    )


async def _ensure_direct_orders(cur, bot_name: str) -> None:
    await cur.execute(
        f"CREATE TABLE IF NOT EXISTS {_q(bot_name, 'swarm_direct_orders')} ("
        "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        "bot_name VARCHAR(50) NOT NULL, "
        "guild_id BIGINT NOT NULL, "
        "vc_id BIGINT NULL, "
        "text_channel_id BIGINT NULL, "
        "command VARCHAR(50) NOT NULL, "
        "data TEXT NULL, "
        "claimed_at TIMESTAMP NULL, "
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        "INDEX idx_unclaimed (bot_name, guild_id, claimed_at, id))"
    )


async def _insert_direct_order(cur, bot_name: str, guild_id: int, vc_id: int | None, text_channel_id: int | None, command: str, data: str | None = None) -> None:
    await _ensure_direct_orders(cur, bot_name)
    await cur.execute(
        f"INSERT INTO {_q(bot_name, 'swarm_direct_orders')} "
        "(bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
        (bot_name, int(guild_id or 0), vc_id, text_channel_id, command.upper(), data or ""),
    )


async def _get_playback_state(cur, bot_name: str, guild_id: int) -> dict[str, Any] | None:
    cols = await _columns(cur, bot_name, "playback_state")
    if not cols:
        return None
    select_cols = ["guild_id"]
    for c in ("is_playing", "is_paused", "connected", "voice_connected", "channel_id", "vc_id", "current_title", "title", "track_title", "video_title", "current_url", "video_url", "updated_at", "last_update", "last_heartbeat"):
        if c in cols:
            select_cols.append(c)
    await cur.execute(
        f"SELECT {', '.join('`'+c+'`' for c in select_cols)} FROM {_q(bot_name, 'playback_state')} WHERE guild_id = %s LIMIT 1",
        (guild_id,),
    )
    return await cur.fetchone()


async def _current_track(cur, bot_name: str, guild_id: int, state: dict[str, Any] | None = None) -> str:
    state = state or await _get_playback_state(cur, bot_name, guild_id) or {}
    for key in ("current_title", "track_title", "video_title", "title"):
        value = state.get(key) if isinstance(state, dict) else None
        if value:
            return str(value)
    try:
        await cur.execute(f"SELECT title FROM {_q(bot_name, 'queue')} WHERE guild_id = %s ORDER BY id ASC LIMIT 1", (guild_id,))
        row = await cur.fetchone()
        if row:
            return str(row.get("title") if isinstance(row, dict) else row[0])
    except Exception:
        pass
    return "Unknown / no live title in DB"


async def _queue_len(cur, bot_name: str, guild_id: int) -> int:
    try:
        await cur.execute(f"SELECT COUNT(*) AS q_len FROM {_q(bot_name, 'queue')} WHERE guild_id = %s", (guild_id,))
        row = await cur.fetchone()
        return int((row.get("q_len") if isinstance(row, dict) else row[0]) or 0)
    except Exception:
        return 0


async def _home_channel(cur, bot_name: str, guild_id: int) -> int | None:
    try:
        await cur.execute(f"SELECT home_vc_id FROM {_q(bot_name, 'bot_home_channels')} WHERE guild_id = %s", (guild_id,))
        row = await cur.fetchone()
        if row:
            return int(row.get("home_vc_id") if isinstance(row, dict) else row[0])
    except Exception:
        return None
    return None


class SwarmAdmin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.medic_task.start()

    def cog_unload(self):
        self.medic_task.cancel()

    @tasks.loop(minutes=2)
    async def medic_task(self):
        if not getattr(db, "pool", None):
            logger.warning("Swarm medic skipped: database pool is unavailable.")
            return
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in DRONE_NAMES:
                        try:
                            await cur.execute(f"SHOW TABLES IN `{_db_name(b)}` LIKE 'swarm_health'")
                            if not await cur.fetchone():
                                continue
                            await cur.execute(f"SELECT bot_name FROM `{_db_name(b)}`.`swarm_health` WHERE last_pulse < NOW() - INTERVAL 3 MINUTE")
                            for dbot in await cur.fetchall():
                                name = dbot.get('bot_name', b)
                                await send_webhook_log("⚠️ Medic Alert: Node Down", f"Node `{name}` missed its heartbeat. It may have crashed, lost DB access, or stopped its health writer.", color=discord.Color.red(), username="Aria Swarm Watch")
                        except Exception as exc:
                            logger.debug("Medic check failed for %s: %s", b, exc)
        except Exception as exc:
            logger.exception("Swarm medic failed: %s", exc)

    @medic_task.before_loop
    async def before_medic(self):
        await self.bot.wait_until_ready()

    swarm_group = app_commands.Group(name="swarm", description="Admin controls for routing and managing the music bot swarm.", default_permissions=discord.Permissions(administrator=True))

    @swarm_group.command(name="radar", description="Inspect each node and see what every music bot is doing from live DB state.")
    async def radar(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if not getattr(db, "pool", None):
            return await interaction.followup.send("❌ Aria has no database pool. Check DB_HOST/DB_PORT/DB_USER/DB_PASSWORD and discord_aria access.")
        embed = discord.Embed(title="📡 Global Swarm Radar", color=discord.Color.brand_green())
        active_nodes = 0
        diagnostics = []
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for d in DRONE_NAMES:
                        try:
                            state = await _get_playback_state(cur, d, int(interaction.guild_id or 0))
                            if not state:
                                diagnostics.append(f"`{d}`: no playback_state row/table")
                                continue
                            active_nodes += 1
                            is_playing = bool(state.get("is_playing")) and not bool(state.get("is_paused", False))
                            connected = state.get("connected", state.get("voice_connected", None))
                            track = await _current_track(cur, d, int(interaction.guild_id or 0), state)
                            q_len = await _queue_len(cur, d, int(interaction.guild_id or 0))
                            channel_id = state.get("channel_id") or state.get("vc_id")
                            freshness = state.get("updated_at") or state.get("last_update") or state.get("last_heartbeat") or "unknown"
                            embed.add_field(
                                name=f"🤖 {d.capitalize()}",
                                value=(
                                    f"{'▶️ **Playing**' if is_playing else '⏸️ **Idle/Paused**'}"
                                    f"\n🔌 **Connected:** {connected if connected is not None else 'unknown'}"
                                    f"\n🎵 **Track:** {track}"
                                    f"\n📋 **Queue:** {q_len}"
                                    f"\n📍 **Channel:** {channel_id or 'unknown'}"
                                    f"\n🕒 **DB Freshness:** {freshness}"
                                ),
                                inline=False,
                            )
                        except Exception as exc:
                            diagnostics.append(f"`{d}`: {type(exc).__name__}: {exc}")
                            logger.exception("Radar failed for %s", d)
            if active_nodes == 0:
                details = "\n".join(diagnostics[:8]) or "No node state found."
                return await interaction.followup.send(f"📡 **No live swarm state found for this server.**\n{details}")
            if diagnostics:
                embed.set_footer(text="Some nodes could not be read. Use /swarm dbcheck for details.")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Radar error: {e}")

    @swarm_group.command(name="dbcheck", description="Check whether Aria can read/write each music bot database table used by the panel.")
    async def dbcheck(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if not getattr(db, "pool", None):
            return await interaction.followup.send("❌ DB pool is missing. Aria is not connected to MariaDB.")
        lines = []
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in DRONE_NAMES:
                        try:
                            await cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s", (_db_name(b),))
                            if not await cur.fetchone():
                                lines.append(f"❌ `{b}` database missing: `{_db_name(b)}`")
                                continue
                            pieces = []
                            for suffix in ("playback_state", "queue", "swarm_overrides", "swarm_direct_orders", "bot_home_channels"):
                                pieces.append(f"{suffix}:{'yes' if await _table_exists(cur, b, suffix) else 'no'}")
                            lines.append(f"✅ `{b}` " + ", ".join(pieces))
                        except Exception as exc:
                            lines.append(f"❌ `{b}` {type(exc).__name__}: {exc}")
            await interaction.followup.send("**Swarm DB Check**\n" + "\n".join(lines)[:1900])
        except Exception as e:
            await interaction.followup.send(f"❌ DB check failed: {e}")

    @swarm_group.command(name="execute", description="Push a playback override command into one or more swarm nodes.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(command=[
        app_commands.Choice(name="Pause", value="PAUSE"), app_commands.Choice(name="Resume", value="RESUME"),
        app_commands.Choice(name="Skip", value="SKIP"), app_commands.Choice(name="Killswitch (Stop & Clear)", value="STOP"),
        app_commands.Choice(name="Restart Node", value="RESTART"), app_commands.Choice(name="Recover", value="RECOVER"),
    ])
    async def execute(self, interaction: discord.Interaction, command: app_commands.Choice[str], drone: app_commands.Choice[str] = None, server_id: str = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        target_guild_ids: list[int] = []
        if server_id:
            try:
                target_guild_ids = [int(server_id)]
            except ValueError:
                return await interaction.followup.send("❌ server_id must be numeric.")
        elif interaction.guild_id:
            target_guild_ids = [int(interaction.guild_id)]
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    writes = 0
                    for b in bots:
                        guild_ids = list(target_guild_ids)
                        if not guild_ids:
                            try:
                                await cur.execute(f"SELECT guild_id FROM {_q(b, 'playback_state')}")
                                guild_ids = [int(row.get("guild_id")) for row in await cur.fetchall() if row.get("guild_id")]
                            except Exception:
                                guild_ids = []
                        if command.value == "RESTART" and not guild_ids:
                            guild_ids = [0]
                        for gid in sorted(set(guild_ids)):
                            await _push_override(cur, b, gid, command.value)
                            writes += 1
                    await conn.commit()
            await interaction.followup.send(f"☢️ **ROUTER OVERRIDE:** committed `{command.value}` to {writes} route(s)." if writes else "⚠️ No target guilds found.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="direct", description="Send a direct play, pause, resume, skip, stop, recover, or leave order to a specific bot.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(action=[
        app_commands.Choice(name="Summon & Play", value="PLAY"), app_commands.Choice(name="Pause", value="PAUSE"),
        app_commands.Choice(name="Resume", value="RESUME"), app_commands.Choice(name="Skip", value="SKIP"),
        app_commands.Choice(name="Stop", value="STOP"), app_commands.Choice(name="Recover", value="RECOVER"),
        app_commands.Choice(name="Force Leave", value="LEAVE"),
    ])
    async def direct(self, interaction: discord.Interaction, drone: app_commands.Choice[str], action: app_commands.Choice[str], data: str = None):
        await interaction.response.defer(ephemeral=True)
        b = drone.value
        target_vc_id = interaction.user.voice.channel.id if interaction.user.voice and interaction.user.voice.channel else None
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if action.value in {"PAUSE", "RESUME", "SKIP", "STOP"}:
                        await _push_override(cur, b, int(interaction.guild_id or 0), action.value)
                        await conn.commit()
                        return await interaction.followup.send(f"📡 Committed `{action.value}` override into `{drone.name}`.")
                    if not target_vc_id:
                        target_vc_id = await _home_channel(cur, b, int(interaction.guild_id or 0))
                    if not target_vc_id and action.value in {"PLAY", "RECOVER"}:
                        return await interaction.followup.send("❌ Join a voice/stage channel or set a Home Channel first.")
                    await _insert_direct_order(cur, b, int(interaction.guild_id or 0), target_vc_id, interaction.channel_id, action.value, data or "")
                    await conn.commit()
            await interaction.followup.send(f"📡 **Telepathic Link:** committed `{action.value}` order into `{drone.name}`.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="set_home", description="Assign a default voice or stage channel to one swarm node.")
    @app_commands.choices(drone=DRONES)
    async def set_home(self, interaction: discord.Interaction, drone: app_commands.Choice[str], channel: discord.VoiceChannel | discord.StageChannel):
        await interaction.response.defer(ephemeral=True)
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {_q(b, 'bot_home_channels')} ("
                        "guild_id BIGINT NOT NULL, bot_name VARCHAR(50) NOT NULL, home_vc_id BIGINT NOT NULL, "
                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, bot_name))"
                    )
                    await cur.execute(f"REPLACE INTO {_q(b, 'bot_home_channels')} (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)", (interaction.guild_id, b, channel.id))
                    await conn.commit()
            await interaction.followup.send(f"🏠 **Homing Beacon Set:** committed to `{drone.name}`.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="broadcast", description="Deploy all active bots to play the same track at once.")
    async def broadcast(self, interaction: discord.Interaction, url: str):
        await interaction.response.defer(ephemeral=True)
        try:
            deployed = 0
            missing = []
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for d in DRONE_NAMES:
                        vc = await _home_channel(cur, d, int(interaction.guild_id or 0))
                        if vc:
                            await _insert_direct_order(cur, d, int(interaction.guild_id or 0), vc, interaction.channel_id, "PLAY", url)
                            deployed += 1
                        else:
                            missing.append(d)
                    await conn.commit()
            if deployed == 0:
                return await interaction.followup.send("❌ No bots have Home Channels set. Use `/swarm set_home` first.")
            msg = f"🚨 **SWARM BROADCAST:** committed PLAY order into **{deployed}** node(s)."
            if missing:
                msg += f" Missing homes: {', '.join(missing)}"
            await interaction.followup.send(msg)
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="purge", description="Clear queued tracks and push a stop override to targeted nodes.")
    @app_commands.choices(drone=DRONES)
    async def purge(self, interaction: discord.Interaction, drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in bots:
                        await _push_override(cur, b, int(interaction.guild_id or 0), "STOP")
                        try:
                            await cur.execute(f"DELETE FROM {_q(b, 'queue')} WHERE guild_id = %s", (interaction.guild_id,))
                        except Exception as exc:
                            logger.debug("Queue purge skipped for %s: %s", b, exc)
                        try:
                            await cur.execute(f"DELETE FROM {_q(b, 'queue_backup')} WHERE guild_id = %s", (interaction.guild_id,))
                        except Exception:
                            pass
                    await conn.commit()
            await interaction.followup.send(f"☢️ **Purge complete:** committed STOP overrides and queue cleanup for {len(bots)} node(s).")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="undo", description="Restore the most recent backup queue for a specific swarm node.")
    @app_commands.choices(drone=DRONES)
    async def undo(self, interaction: discord.Interaction, drone: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        b = drone.value
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(f"SELECT * FROM {_q(b, 'queue_backup')} WHERE guild_id = %s ORDER BY id DESC LIMIT 20", (interaction.guild_id,))
                    backups = await cur.fetchall()
                    if not backups:
                        return await interaction.followup.send(f"❌ No recent backups found for `{drone.name}`.")
                    for t in reversed(backups):
                        await cur.execute(f"INSERT INTO {_q(b, 'queue')} (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)", (t['guild_id'], b, t.get('video_url'), t.get('title'), interaction.user.id))
                    await conn.commit()
            await interaction.followup.send(f"⏪ **Archivist Restored:** recovered queue backup for `{drone.name}`.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="wrapped", description="View the most-played tracks across the swarm for this server.")
    async def wrapped(self, interaction: discord.Interaction):
        await interaction.response.defer()
        track_counts: dict[str, int] = {}
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in DRONE_NAMES:
                        try:
                            await cur.execute(f"SELECT title FROM {_q(b, 'history')} WHERE guild_id = %s", (interaction.guild_id,))
                            for row in await cur.fetchall():
                                title = row.get('title') or 'Unknown'
                                track_counts[title] = track_counts.get(title, 0) + 1
                        except Exception:
                            pass
            if not track_counts:
                return await interaction.followup.send("📊 Not enough analytics data collected yet.")
            desc = "".join(f"**{i}.** {title} — `{count} plays`\n" for i, (title, count) in enumerate(sorted(track_counts.items(), key=lambda x: x[1], reverse=True)[:5], 1))
            await interaction.followup.send(embed=discord.Embed(title="📊 Server Wrapped: Top Swarm Tracks", description=desc, color=discord.Color.gold()))
        except Exception as e:
            await interaction.followup.send(f"❌ Analytics Error: {e}")

    @swarm_group.command(name="loop", description="Change the loop mode for one bot or the whole swarm.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(mode=[app_commands.Choice(name="Off (Standard Playback)", value="off"), app_commands.Choice(name="Song (Loop current track)", value="song"), app_commands.Choice(name="Queue (Loop entire queue)", value="queue")])
    async def loop(self, interaction: discord.Interaction, mode: app_commands.Choice[str], drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        await ensure_guild_settings_schema(cur, b)
                        await cur.execute(f"INSERT INTO {_q(b, 'guild_settings')} (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s", (interaction.guild_id, mode.value, mode.value))
                    await conn.commit()
            await interaction.followup.send(f"🔁 Loop set to `{mode.name}` for {'`'+drone.name+'`' if drone else 'ALL nodes'}.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="queue", description="View the upcoming songs queued on a specific swarm node.")
    @app_commands.choices(drone=DRONES)
    async def view_queue(self, interaction: discord.Interaction, drone: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        b = drone.value
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(f"SELECT title FROM {_q(b, 'queue')} WHERE guild_id = %s ORDER BY id ASC LIMIT 15", (interaction.guild_id,))
                    tracks = await cur.fetchall()
            if not tracks:
                return await interaction.followup.send(f"📭 `{drone.name}` queue is empty.")
            desc = "".join(f"**{i}.** {t.get('title') or 'Unknown'}\n" for i, t in enumerate(tracks, 1))
            await interaction.followup.send(embed=discord.Embed(title=f"📋 Upcoming Tracks: {drone.name}", description=desc, color=discord.Color.blurple()))
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="shuffle", description="Shuffle the queued tracks on one bot or across the swarm.")
    @app_commands.choices(drone=DRONES)
    async def shuffle(self, interaction: discord.Interaction, drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        shuffled, not_enough, missing_queue = [], [], []
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in bots:
                        try:
                            await cur.execute(f"SELECT * FROM {_q(b, 'queue')} WHERE guild_id = %s ORDER BY id ASC", (interaction.guild_id,))
                            tracks = await cur.fetchall()
                        except Exception:
                            missing_queue.append(b); continue
                        if len(tracks) < 2:
                            not_enough.append(b); continue
                        random.shuffle(tracks)
                        await cur.execute(f"DELETE FROM {_q(b, 'queue')} WHERE guild_id = %s", (interaction.guild_id,))
                        for t in tracks:
                            await cur.execute(f"INSERT INTO {_q(b, 'queue')} (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)", (t.get('guild_id'), t.get('bot_name', b), t.get('video_url'), t.get('title'), t.get('requester_id')))
                        shuffled.append(b)
                    await conn.commit()
            messages = []
            if shuffled: messages.append(f"🔀 Shuffled: {', '.join(shuffled)}")
            if not_enough: messages.append(f"⚠️ Not enough tracks: {', '.join(not_enough)}")
            if missing_queue: messages.append(f"📭 Queue unavailable: {', '.join(missing_queue)}")
            await interaction.followup.send("\n".join(messages) or "⚠️ No queues could be shuffled.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="remove", description="Remove a specific track number from a bot's queue.")
    @app_commands.choices(drone=DRONES)
    async def remove(self, interaction: discord.Interaction, drone: app_commands.Choice[str], track_number: int):
        await interaction.response.defer(ephemeral=True)
        b = drone.value
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(f"SELECT id, title FROM {_q(b, 'queue')} WHERE guild_id = %s ORDER BY id ASC", (interaction.guild_id,))
                    tracks = await cur.fetchall()
                    if track_number < 1 or track_number > len(tracks):
                        return await interaction.followup.send(f"❌ Invalid track number. `{drone.name}` has {len(tracks)} queued tracks.")
                    target = tracks[track_number - 1]
                    await cur.execute(f"DELETE FROM {_q(b, 'queue')} WHERE id = %s AND guild_id = %s", (target['id'], interaction.guild_id))
                    await conn.commit()
            await interaction.followup.send(f"✂️ Removed `{target.get('title')}` from `{drone.name}` queue.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="filter", description="Set the audio filter for a specific bot or the whole swarm.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(filter_type=[app_commands.Choice(name="High Quality (Unfiltered)", value="none"), app_commands.Choice(name="Nightcore (Fast/High Pitch)", value="nightcore"), app_commands.Choice(name="Vaporwave (Slow/Reverb)", value="vaporwave"), app_commands.Choice(name="Bassboost", value="bassboost"), app_commands.Choice(name="8D Audio (Panning)", value="8d")])
    async def filter_cmd(self, interaction: discord.Interaction, filter_type: app_commands.Choice[str], drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        await ensure_guild_settings_schema(cur, b)
                        await cur.execute(f"INSERT INTO {_q(b, 'guild_settings')} (guild_id, filter_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE filter_mode = %s", (interaction.guild_id, filter_type.value, filter_type.value))
                        await _push_override(cur, b, int(interaction.guild_id or 0), "UPDATE_FILTER")
                    await conn.commit()
            await interaction.followup.send(f"🎛️ Filter `{filter_type.name}` committed for {'`'+drone.name+'`' if drone else 'ALL nodes'}.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}")


async def setup(bot):
    await bot.add_cog(SwarmAdmin(bot))
