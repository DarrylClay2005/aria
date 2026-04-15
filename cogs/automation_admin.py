import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiomysql
import logging
import json
import io
from datetime import datetime, timedelta
from core.database import db

logger = logging.getLogger("discord")

class AutomationAdmin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.auto_message_loop.start()

    def cog_unload(self):
        self.auto_message_loop.cancel()

    # --- DATABASE INIT ---
    async def cog_load(self):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS aria_automations (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        guild_id BIGINT,
                        channel_id BIGINT,
                        message TEXT,
                        interval_hours INT,
                        last_sent TIMESTAMP NULL DEFAULT NULL
                    )
                """)

    # --- AUTOMATED MESSAGE LOOP ---
    @tasks.loop(minutes=30.0) 
    async def auto_message_loop(self):
        async with db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM aria_automations")
                automations = await cur.fetchall()

                now = datetime.now()
                for auto in automations:
                    if not auto['last_sent'] or (now - auto['last_sent']) >= timedelta(hours=auto['interval_hours']):
                        channel = self.bot.get_channel(auto['channel_id'])
                        if channel:
                            try:
                                await channel.send(auto['message'])
                                await cur.execute("UPDATE aria_automations SET last_sent = %s WHERE id = %s", (now, auto['id']))
                            except discord.Forbidden:
                                logger.error(f"Aria lacks permission to send auto-message in {channel.name}")

    @auto_message_loop.before_loop
    async def before_auto_message_loop(self):
        await self.bot.wait_until_ready()

    # --- AUTOMATION COMMANDS ---
    auto_group = app_commands.Group(name="auto_message", description="[OWNER] Manage Aria's automated recurring messages")

    @auto_group.command(name="set", description="Set up a new recurring automated message")
    @app_commands.default_permissions(administrator=True)
    async def auto_set(self, interaction: discord.Interaction, channel: discord.TextChannel, interval_hours: int, message: str):
        if interval_hours < 1:
            return await interaction.response.send_message("I'm not spamming a channel every few minutes. Minimum interval is 1 hour.", ephemeral=True)

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO aria_automations (guild_id, channel_id, message, interval_hours) VALUES (%s, %s, %s, %s)", 
                    (interaction.guild.id, channel.id, message, interval_hours)
                )
                    
        await interaction.response.send_message(f"Fucking fine. I'll automatically post that message in {channel.mention} every **{interval_hours} hours**. Apparently, you humans are too forgetful to do it yourselves.")

    @auto_group.command(name="list", description="List all automated messages in this server")
    @app_commands.default_permissions(administrator=True)
    async def auto_list(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM aria_automations WHERE guild_id = %s", (interaction.guild.id,))
                results = await cur.fetchall()

        if not results:
            return await interaction.response.send_message("You have no automated messages running. Good. Less work for me.", ephemeral=True)

        desc = ""
        for r in results:
            ch = self.bot.get_channel(r['channel_id'])
            ch_name = ch.mention if ch else f"Deleted Channel ({r['channel_id']})"
            desc += f"**ID: {r['id']}** | Every {r['interval_hours']}h | {ch_name}\n> {r['message'][:50]}...\n\n"

        embed = discord.Embed(title="⚙️ Aria's Automated Schedules", description=desc, color=discord.Color.dark_grey())
        await interaction.response.send_message(embed=embed)

    @auto_group.command(name="remove", description="Remove an automated message by its ID")
    @app_commands.default_permissions(administrator=True)
    async def auto_remove(self, interaction: discord.Interaction, auto_id: int):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM aria_automations WHERE id = %s AND guild_id = %s", (auto_id, interaction.guild.id))
                if cur.rowcount == 0:
                    return await interaction.response.send_message("I couldn't find an automation with that ID in this server. Are you blind?", ephemeral=True)

        await interaction.response.send_message(f"Automation #{auto_id} deleted. I will no longer waste my breath on it.", ephemeral=False)

    # --- MASS ADMINISTRATION COMMANDS ---
    @app_commands.command(name="mass_role_add", description="[OWNER] Add a specific role to multiple users at once")
    @app_commands.default_permissions(administrator=True)
    async def mass_role_add(self, interaction: discord.Interaction, role: discord.Role, user_ids: str):
        await interaction.response.defer(ephemeral=True)
        id_list = user_ids.split()
        success = 0
        
        for uid in id_list:
            try:
                member = interaction.guild.get_member(int(uid)) or await interaction.guild.fetch_member(int(uid))
                if member:
                    await member.add_roles(role, reason="Aria's mass role sweep")
                    success += 1
            except Exception:
                pass

        await interaction.followup.send(f"Done. I aggressively slapped the **{role.name}** role onto {success} users out of the {len(id_list)} IDs provided.")

    @app_commands.command(name="mass_channel_create", description="[OWNER] Create multiple identical text channels at once")
    @app_commands.default_permissions(administrator=True)
    async def mass_channel_create(self, interaction: discord.Interaction, base_name: str, count: int, category: discord.CategoryChannel = None):
        if count > 20:
            return await interaction.response.send_message("I am not creating more than 20 channels at once. Do you want to hit a rate limit? Because that's how you hit a rate limit.", ephemeral=True)
            
        await interaction.response.defer(ephemeral=False)
        
        for i in range(1, count + 1):
            name = f"{base_name}-{i}"
            try:
                await interaction.guild.create_text_channel(name=name, category=category, reason="Aria's mass channel creation")
            except discord.Forbidden:
                return await interaction.followup.send("I don't have permission to build channels here. Fix my roles.")
                
        await interaction.followup.send(f"For fuck's sake. I just built **{count}** '{base_name}' channels for you. I hope you're happy, because I'm exhausted.")

    @app_commands.command(name="member_audit", description="[OWNER] Get detailed security information about a specific member")
    @app_commands.default_permissions(administrator=True)
    async def member_audit(self, interaction: discord.Interaction, target: discord.Member):
        embed = discord.Embed(title=f"🔍 Security Audit: {target.display_name}", color=discord.Color.dark_red())
        embed.set_thumbnail(url=target.display_avatar.url)
        embed.add_field(name="Account Created", value=f"<t:{int(target.created_at.timestamp())}:R>", inline=True)
        embed.add_field(name="Joined Server", value=f"<t:{int(target.joined_at.timestamp())}:R>", inline=True)
        embed.add_field(name="Bot Status", value="Yes" if target.bot else "No", inline=True)
        
        roles = [r.mention for r in target.roles if r != interaction.guild.default_role]
        embed.add_field(name=f"Roles ({len(roles)})", value=" ".join(roles) if roles else "None", inline=False)
        
        if target.guild_permissions.administrator:
            embed.add_field(name="⚠️ WARNING", value="This user has full Administrator privileges.", inline=False)
            
        embed.set_footer(text="Aria sees all. Aria judges all.")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="server_backup", description="[OWNER] Export a JSON file of your server's roles and channels")
    @app_commands.default_permissions(administrator=True)
    async def server_backup(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        data = {
            "server_name": guild.name,
            "server_id": guild.id,
            "roles": [{"id": r.id, "name": r.name, "color": str(r.color)} for r in guild.roles],
            "channels": [{"id": c.id, "name": c.name, "type": str(c.type)} for c in guild.channels]
        }
        
        json_data = json.dumps(data, indent=4)
        file = discord.File(io.BytesIO(json_data.encode()), filename=f"backup_{guild.id}.json")
        
        await interaction.followup.send("Here is the blueprint of your server. Try not to lose it, because I'm not rebuilding it for you if you accidentally nuke everything.", file=file)

async def setup(bot):
    await bot.add_cog(AutomationAdmin(bot))