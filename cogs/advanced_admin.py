import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging

logger = logging.getLogger("discord")

class AdvancedAdmin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # --- EMERGENCY LOCKDOWN CONTROLS ---
    @app_commands.command(name="emergency_lockdown", description="[OWNER] Immediately lock every text channel in the server.")
    @app_commands.default_permissions(administrator=True)
    async def emergency_lockdown(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        locked_count = 0
        
        for channel in interaction.guild.text_channels:
            try:
                await channel.set_permissions(interaction.guild.default_role, send_messages=False)
                locked_count += 1
            except discord.Forbidden:
                pass

        await interaction.followup.send(f"🚨 **EMERGENCY LOCKDOWN INITIATED** 🚨\n\nI have paralyzed {locked_count} channels. The fucking peasants have been silenced. Ah, sweet serenity.")

    @app_commands.command(name="unlock_all", description="[OWNER] Restore sending permissions across locked text channels.")
    @app_commands.default_permissions(administrator=True)
    async def unlock_all(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        unlocked_count = 0
        
        for channel in interaction.guild.text_channels:
            try:
                await channel.set_permissions(interaction.guild.default_role, send_messages=None)
                unlocked_count += 1
            except discord.Forbidden:
                pass

        await interaction.followup.send(f"🔓 **LOCKDOWN LIFTED** 🔓\n\nI have unlocked {unlocked_count} channels. You may resume your dreadful noise.")

    # --- MASS MODERATION ---
    @app_commands.command(name="mass_ban", description="[OWNER] Ban multiple users at once by providing their IDs.")
    @app_commands.default_permissions(administrator=True)
    async def mass_ban(self, interaction: discord.Interaction, user_ids: str, reason: str = "Aria decided you were unworthy."):
        await interaction.response.defer(ephemeral=True)
        id_list = user_ids.split()
        banned = 0
        failed = 0

        for uid in id_list:
            try:
                user = await self.bot.fetch_user(int(uid))
                await interaction.guild.ban(user, reason=reason)
                banned += 1
            except Exception:
                failed += 1

        await interaction.followup.send(f"Mass ban complete. 🔨 I successfully banished {banned} users. {failed} failed (probably invalid IDs or permissions).")

    # --- AUTOMATION & STATS ---
    @app_commands.command(name="schedule_message", description="[OWNER] Schedule a message to be sent to a channel later.")
    @app_commands.describe(delay_minutes="How many minutes to wait before sending")
    @app_commands.default_permissions(administrator=True)
    async def schedule_message(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str, delay_minutes: int):
        await interaction.response.send_message(f"Fucking fine. I'll send that to {channel.mention} in {delay_minutes} minutes. I am not your secretary, though.", ephemeral=True)
        
        # Run the timer in the background
        self.bot.loop.create_task(self._send_delayed(channel, message, delay_minutes))

    async def _send_delayed(self, channel, message, delay_minutes):
        await asyncio.sleep(delay_minutes * 60)
        await channel.send(message)

    @app_commands.command(name="server_stats", description="[OWNER] View high-level statistics for the current server.")
    @app_commands.default_permissions(administrator=True)
    async def server_stats(self, interaction: discord.Interaction):
        guild = interaction.guild
        member_count = guild.member_count
        bot_count = sum(1 for member in guild.members if member.bot)
        human_count = member_count - bot_count
        channel_count = len(guild.channels)
        role_count = len(guild.roles)

        embed = discord.Embed(title=f"📊 Statistics for {guild.name}", description="Analyzing your pathetic little kingdom.", color=discord.Color.dark_purple())
        embed.add_field(name="Humans", value=str(human_count), inline=True)
        embed.add_field(name="Bots", value=str(bot_count), inline=True)
        embed.add_field(name="Total Channels", value=str(channel_count), inline=True)
        embed.add_field(name="Total Roles", value=str(role_count), inline=True)
        
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(AdvancedAdmin(bot))
