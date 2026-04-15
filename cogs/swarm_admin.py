import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiomysql
import logging
import random
import aiohttp
import os
from core.database import db

class HTTPSessionManager:
    _session = None
    async def __aenter__(self):
        if not HTTPSessionManager._session:
            HTTPSessionManager._session = aiohttp.ClientSession()
        return HTTPSessionManager._session
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

logger = logging.getLogger("discord")
WEBHOOK_URL = os.getenv("ARIA_SWARM_WEBHOOK_URL")

DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream"]
DRONES = [app_commands.Choice(name=d.capitalize(), value=d) for d in DRONE_NAMES]

class SwarmAdmin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.medic_task.start()

    def cog_unload(self):
        self.medic_task.cancel()

    @tasks.loop(minutes=2)
    async def medic_task(self):
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in DRONE_NAMES:
                        try:
                            await cur.execute(f"SELECT bot_name FROM discord_music_{b}.swarm_health WHERE last_pulse < NOW() - INTERVAL 3 MINUTE")
                            dead_bots = await cur.fetchall()
                            for dbot in dead_bots:
                                name = dbot['bot_name']
                                if WEBHOOK_URL:
                                    async with HTTPSessionManager() as session:
                                        webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
                                        embed = discord.Embed(title="⚠️ Medic Alert: Node Down", description=f"Node `{name}` missed its heartbeat. It may have crashed or hit a rate limit. Auto-Failover standing by.", color=discord.Color.red())
                                        await webhook.send(embed=embed)
                        except: pass
        except: pass

    @medic_task.before_loop
    async def before_medic(self):
        await self.bot.wait_until_ready()

    swarm_group = app_commands.Group(name="swarm", description="Admin controls for routing and managing the music bot swarm.", default_permissions=discord.Permissions(administrator=True))

    @swarm_group.command(name="undo", description="Restore the most recent backup queue for a specific swarm node.")
    @app_commands.choices(drone=DRONES)
    async def undo(self, interaction: discord.Interaction, drone: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute(f"SELECT * FROM discord_music_{b}.{b}_queue_backup WHERE guild_id = %s ORDER BY id DESC LIMIT 20", (interaction.guild_id,))
                        backups = await cur.fetchall()
                        if not backups: return await interaction.followup.send(f"❌ No recent backups found for `{drone.name}`.")
                        
                        for t in reversed(backups):
                            await cur.execute(f"INSERT INTO discord_music_{b}.{b}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)", 
                                (t['guild_id'], b, t['video_url'], t['title'], interaction.user.id))
                        await interaction.followup.send(f"⏪ **Archivist Restored:** Recovered the last known queue state for `{drone.name}`.")
                    except Exception as inner_e:
                        await interaction.followup.send(f"❌ No backup table found for `{drone.name}` yet. Play some tracks first!")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="wrapped", description="View the most-played tracks across the swarm for this server.")
    async def wrapped(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            track_counts = {}
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in DRONE_NAMES:
                        try:
                            await cur.execute(f"SELECT title FROM discord_music_{b}.{b}_history WHERE guild_id = %s", (interaction.guild_id,))
                            for row in await cur.fetchall():
                                t = row['title']
                                track_counts[t] = track_counts.get(t, 0) + 1
                        except: pass
                            
            if not track_counts: return await interaction.followup.send("📊 Not enough analytics data collected yet. Keep playing music!")
            
            top_tracks = sorted(track_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            desc = ""
            for i, (title, count) in enumerate(top_tracks, 1):
                desc += f"**{i}.** {title} — `{count} plays`\n"
            
            embed = discord.Embed(title="📊 Server Wrapped: Top Swarm Tracks", description=desc, color=discord.Color.gold())
            await interaction.followup.send(embed=embed)
        except Exception as e: await interaction.followup.send(f"❌ Analytics Error: {e}")

    @swarm_group.command(name="radar", description="Inspect each node and see what every music bot is doing.")
    async def radar(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        embed = discord.Embed(title="📡 Global Swarm Radar (Quarantined Mode)", color=discord.Color.brand_green())
        active_nodes = 0
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for d in DRONE_NAMES:
                        try:
                            await cur.execute(f"SHOW TABLES IN discord_music_{d} LIKE '{d}_playback_state'")
                            if not await cur.fetchone(): continue

                            await cur.execute(f"SELECT guild_id, is_playing FROM discord_music_{d}.{d}_playback_state WHERE guild_id = %s LIMIT 1", (interaction.guild_id,))
                            state = await cur.fetchone()
                            if state:
                                active_nodes += 1
                                gid = state['guild_id']
                                is_p = state['is_playing']
                                await cur.execute(f"SELECT title FROM discord_music_{d}.{d}_queue WHERE guild_id = %s ORDER BY id ASC LIMIT 1", (interaction.guild_id,))
                                q_top = await cur.fetchone()
                                track = q_top['title'] if q_top else "Unknown Track"
                                await cur.execute(f"SELECT COUNT(*) as q_len FROM discord_music_{d}.{d}_queue WHERE guild_id = %s", (interaction.guild_id,))
                                q_len = (await cur.fetchone())['q_len'] or 0
                                g_name = interaction.client.get_guild(gid).name if interaction.client.get_guild(gid) else f"Server: {gid}"
                                embed.add_field(name=f"🏢 {g_name} | 🤖 {d.capitalize()}", value=f"{'▶️ **Playing**' if is_p else '⏸️ **Paused**'}\n🎵 **Track:** {track}\n📋 **Queue:** {q_len} left", inline=False)
                        except: pass
            
            if active_nodes == 0: return await interaction.followup.send("📡 **Grid is quiet.** All isolated tables report no active playback.")
            await interaction.followup.send(embed=embed)
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="execute", description="Push a playback override command into one or more swarm nodes.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(command=[
        app_commands.Choice(name="Pause", value="PAUSE"), app_commands.Choice(name="Resume", value="RESUME"),
        app_commands.Choice(name="Skip", value="SKIP"), app_commands.Choice(name="Killswitch (Stop & Clear)", value="STOP")
    ])
    async def execute(self, interaction: discord.Interaction, command: app_commands.Choice[str], drone: app_commands.Choice[str] = None, server_id: str = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                        if server_id: await cur.execute(f"REPLACE INTO discord_music_{b}.{b}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)", (int(server_id), b, command.value))
                        else:
                            try:
                                await cur.execute(f"SELECT guild_id FROM discord_music_{b}.{b}_playback_state")
                                for g in await cur.fetchall(): await cur.execute(f"REPLACE INTO discord_music_{b}.{b}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)", (g[0], b, command.value))
                            except: pass
            await interaction.followup.send(f"☢️ **ROUTER OVERRIDE:** Pushed **{command.value}** protocol to {len(bots)} isolated tables.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="purge", description="Clear queued tracks and push a stop override to targeted nodes.")
    @app_commands.choices(drone=DRONES)
    async def purge(self, interaction: discord.Interaction, drone: app_commands.Choice[str] = None):
        try:
            await interaction.response.defer(ephemeral=True)
        except discord.NotFound:
            pass
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        try:
                            await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                            target_guild_ids = []
                            try:
                                await cur.execute(f"SELECT guild_id FROM discord_music_{b}.{b}_playback_state")
                                target_guild_ids = [row[0] for row in await cur.fetchall()]
                            except Exception:
                                pass
                            if interaction.guild_id and interaction.guild_id not in target_guild_ids:
                                target_guild_ids.append(interaction.guild_id)
                            for gid in target_guild_ids:
                                await cur.execute(f"REPLACE INTO discord_music_{b}.{b}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)", (gid, b, "STOP"))
                            await cur.execute(f"DELETE FROM discord_music_{b}.{b}_queue WHERE guild_id = %s", (interaction.guild_id,))
                        except:
                            pass
            success_msg = f"☢️ **Purge complete across {len(bots)} isolated tables:** queues wiped and active playback stop overrides pushed."
            try:
                await interaction.followup.send(success_msg)
            except discord.NotFound:
                if interaction.channel:
                    await interaction.channel.send(success_msg)
        except Exception as e:
            err_msg = f"❌ Error: {e}"
            try:
                await interaction.followup.send(err_msg)
            except discord.NotFound:
                if interaction.channel:
                    await interaction.channel.send(err_msg)

    @swarm_group.command(name="direct", description="Send a direct play or leave order to a specific bot.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(action=[app_commands.Choice(name="Summon & Play", value="PLAY"), app_commands.Choice(name="Force Leave", value="LEAVE")])
    async def direct(self, interaction: discord.Interaction, drone: app_commands.Choice[str], action: app_commands.Choice[str], data: str = None):
        await interaction.response.defer(ephemeral=True)
        if action.value == "PLAY" and not data: return await interaction.followup.send("❌ Provide a URL.")
        target_vc_id = interaction.user.voice.channel.id if interaction.user.voice and interaction.user.voice.channel else None
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if not target_vc_id:
                        try:
                            await cur.execute(f"SELECT home_vc_id FROM discord_music_{b}.{b}_bot_home_channels WHERE guild_id = %s", (interaction.guild_id,))
                            res = await cur.fetchone()
                            if res: target_vc_id = res.get('home_vc_id')
                        except: pass
                    if not target_vc_id: return await interaction.followup.send("❌ Join a Channel or set a Home Channel first.")
                    
                    await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)")
                    await cur.execute(f"INSERT INTO discord_music_{b}.{b}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
                        (b, interaction.guild_id, target_vc_id, interaction.channel_id, action.value, data or ""))
            await interaction.followup.send(f"📡 **Telepathic Link:** Routed order straight into `{drone.name}`'s quarantined database table.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="broadcast", description="Deploy all active bots to play the same track at once.")
    async def broadcast(self, interaction: discord.Interaction, url: str):
        await interaction.response.defer(ephemeral=True)
        try:
            deployed = 0
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for d in DRONE_NAMES:
                        try:
                            await cur.execute(f"SELECT home_vc_id FROM discord_music_{d}.{d}_bot_home_channels WHERE guild_id = %s", (interaction.guild_id,))
                            res = await cur.fetchone()
                            if res and res.get('home_vc_id'):
                                await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{d}.{d}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)")
                                await cur.execute(f"INSERT INTO discord_music_{d}.{d}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (d, interaction.guild_id, res['home_vc_id'], interaction.channel_id, "PLAY", url))
                                deployed += 1
                        except: pass
            if deployed == 0: await interaction.followup.send("❌ No bots have Home Channels set. They don't know where to deploy!")
            else: await interaction.followup.send(f"🚨 **SWARM BROADCAST:** Payload successfully injected into **{deployed}** nodes.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="set_home", description="Assign a default voice or stage channel to one swarm node.")
    @app_commands.choices(drone=DRONES)
    async def set_home(self, interaction: discord.Interaction, drone: app_commands.Choice[str], channel: discord.VoiceChannel | discord.StageChannel):
        await interaction.response.defer(ephemeral=True)
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                    await cur.execute(f"REPLACE INTO discord_music_{b}.{b}_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)", (interaction.guild_id, b, channel.id))
            await interaction.followup.send(f"🏠 **Homing Beacon Set:** Written to `{drone.name}`'s isolated table.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="loop", description="Change the loop mode for one bot or the whole swarm.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(mode=[
        app_commands.Choice(name="Off (Standard Playback)", value="off"),
        app_commands.Choice(name="Song (Loop current track)", value="song"),
        app_commands.Choice(name="Queue (Loop entire queue)", value="queue")
    ])
    async def loop(self, interaction: discord.Interaction, mode: app_commands.Choice[str], drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_guild_settings (guild_id BIGINT PRIMARY KEY, loop_mode VARCHAR(10) DEFAULT 'off', volume INT DEFAULT 100, filter_mode VARCHAR(20) DEFAULT 'none')")
                        await cur.execute(f"INSERT INTO discord_music_{b}.{b}_guild_settings (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s", (interaction.guild_id, mode.value, mode.value))
            msg = f"🔁 **Loop Mode:** Set to `{mode.name}` for `{drone.name}`." if drone else f"☢️ **GLOBAL LOOP:** Set to `{mode.name}` for ALL nodes."
            await interaction.followup.send(msg)
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="queue", description="View the upcoming songs queued on a specific swarm node.")
    @app_commands.choices(drone=DRONES)
    async def view_queue(self, interaction: discord.Interaction, drone: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute(f"SELECT title FROM discord_music_{b}.{b}_queue WHERE guild_id = %s ORDER BY id ASC LIMIT 15", (interaction.guild_id,))
                        tracks = await cur.fetchall()
                        if not tracks: return await interaction.followup.send(f"📭 `{drone.name}`'s queue is currently empty.")
                        
                        desc = ""
                        for i, t in enumerate(tracks, 1): desc += f"**{i}.** {t['title']}\n"
                        await interaction.followup.send(embed=discord.Embed(title=f"📋 Upcoming Tracks: {drone.name}", description=desc, color=discord.Color.blurple()))
                    except: await interaction.followup.send(f"📭 `{drone.name}`'s queue is currently empty.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="shuffle", description="Shuffle the queued tracks on one bot or across the swarm.")
    @app_commands.choices(drone=DRONES)
    async def shuffle(self, interaction: discord.Interaction, drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            shuffled, not_enough, missing_queue = [], [], []
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for b in bots:
                        try:
                            await cur.execute(f"SELECT * FROM discord_music_{b}.{b}_queue WHERE guild_id = %s", (interaction.guild_id,))
                            tracks = await cur.fetchall()
                        except Exception:
                            missing_queue.append(b)
                            continue

                        if len(tracks) < 2:
                            not_enough.append(b)
                            continue

                        random.shuffle(tracks)
                        await cur.execute(f"DELETE FROM discord_music_{b}.{b}_queue WHERE guild_id = %s", (interaction.guild_id,))
                        for t in tracks:
                            await cur.execute(
                                f"INSERT INTO discord_music_{b}.{b}_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
                                (t['guild_id'], t['bot_name'], t['video_url'], t['title'], t['requester_id'])
                            )
                        shuffled.append(b)

            messages = []
            if shuffled:
                messages.append(f"🔀 **Queue Shuffled:** {', '.join(f'`{name}`' for name in shuffled)}")
            if not_enough:
                messages.append(f"⚠️ **Not enough tracks to shuffle:** {', '.join(f'`{name}`' for name in not_enough)}")
            if missing_queue:
                messages.append(f"📭 **Queue unavailable:** {', '.join(f'`{name}`' for name in missing_queue)}")
            if not messages:
                messages.append("⚠️ No queues could be shuffled.")
            await interaction.followup.send("\n".join(messages))
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="remove", description="Remove a specific track number from a bot's queue.")
    @app_commands.choices(drone=DRONES)
    async def remove(self, interaction: discord.Interaction, drone: app_commands.Choice[str], track_number: int):
        await interaction.response.defer(ephemeral=True)
        try:
            b = drone.value
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute(f"SELECT id, title FROM discord_music_{b}.{b}_queue WHERE guild_id = %s ORDER BY id ASC", (interaction.guild_id,))
                        tracks = await cur.fetchall()
                        if track_number < 1 or track_number > len(tracks):
                            return await interaction.followup.send(f"❌ Invalid track number. `{drone.name}` only has {len(tracks)} tracks in the queue.")
                        
                        target_track = tracks[track_number - 1]
                        await cur.execute(f"DELETE FROM discord_music_{b}.{b}_queue WHERE id = %s", (target_track['id'],))
                        await interaction.followup.send(f"✂️ **Surgically Removed:** Skipped `{target_track['title']}` in `{drone.name}`'s queue.")
                    except: await interaction.followup.send(f"⚠️ `{drone.name}` queue not found.")
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

    @swarm_group.command(name="filter", description="Set the audio filter for a specific bot or the whole swarm.")
    @app_commands.choices(drone=DRONES)
    @app_commands.choices(filter_type=[
        app_commands.Choice(name="High Quality (Unfiltered)", value="none"),
        app_commands.Choice(name="Nightcore (Fast/High Pitch)", value="nightcore"),
        app_commands.Choice(name="Vaporwave (Slow/Reverb)", value="vaporwave"),
        app_commands.Choice(name="Bassboost", value="bassboost"),
        app_commands.Choice(name="8D Audio (Panning)", value="8d")
    ])
    async def filter_cmd(self, interaction: discord.Interaction, filter_type: app_commands.Choice[str], drone: app_commands.Choice[str] = None):
        await interaction.response.defer(ephemeral=True)
        bots = [drone.value] if drone else DRONE_NAMES
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for b in bots:
                        await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_guild_settings (guild_id BIGINT PRIMARY KEY, loop_mode VARCHAR(10) DEFAULT 'off', volume INT DEFAULT 100, filter_mode VARCHAR(20) DEFAULT 'none')")
                        try: await cur.execute(f"ALTER TABLE discord_music_{b}.{b}_guild_settings ADD COLUMN filter_mode VARCHAR(20) DEFAULT 'none'")
                        except: pass
                        await cur.execute(f"INSERT INTO discord_music_{b}.{b}_guild_settings (guild_id, filter_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE filter_mode = %s", (interaction.guild_id, filter_type.value, filter_type.value))
                        await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{b}.{b}_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                        await cur.execute(f"REPLACE INTO discord_music_{b}.{b}_swarm_overrides (guild_id, bot_name, command) VALUES (%s, %s, %s)", (interaction.guild_id, b, "UPDATE_FILTER"))
            msg = f"🎛️ **Audio Filter:** Set to `{filter_type.name}` for `{drone.name}`. Matrix override initiated. Applied instantly." if drone else f"☢️ **GLOBAL FILTER:** Set to `{filter_type.name}` for ALL nodes."
            await interaction.followup.send(msg)
        except Exception as e: await interaction.followup.send(f"❌ Error: {e}")

async def setup(bot): await bot.add_cog(SwarmAdmin(bot))
