import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from datetime import timedelta

logger = logging.getLogger("discord")

class AdvancedAdmin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def _member_block_reason(self, interaction: discord.Interaction, target: discord.Member) -> str | None:
        guild = interaction.guild
        bot_member = guild.me if guild else None
        if not guild or not bot_member:
            return "This command only works inside a server."
        if target == guild.owner:
            return "I am not touching the server owner. Even I have structural limits."
        if target == bot_member:
            return "I am not using moderation tools on myself."
        if target.top_role >= bot_member.top_role:
            return "That member's top role is at or above mine. Move Aria's role higher first."
        return None

    @staticmethod
    def _parse_color(value: str | None) -> discord.Color:
        if not value:
            return discord.Color.default()
        normalized = value.strip().lstrip("#")
        if len(normalized) != 6:
            raise ValueError("Color must be a 6-digit hex value like #8b5cf6.")
        return discord.Color(int(normalized, 16))

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

        await interaction.followup.send(
            f"🚨 **EMERGENCY LOCKDOWN INITIATED** 🚨\n\n"
            f"I have paralyzed {locked_count} channels. The fucking peasants have been silenced. Ah, sweet serenity."
        )

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

        await interaction.followup.send(
            f"🔓 **LOCKDOWN LIFTED** 🔓\n\nI have unlocked {unlocked_count} channels. You may resume your dreadful noise."
        )

    # --- MASS MODERATION ---
    @app_commands.command(name="mass_ban", description="[OWNER] Ban multiple users at once by providing their IDs.")
    @app_commands.default_permissions(administrator=True)
    async def mass_ban(self, interaction: discord.Interaction, user_ids: str, reason: str = "Aria decided you were unworthy."):
        await interaction.response.defer(ephemeral=True)
        id_list = user_ids.split()
        banned = 0
        failed = 0

        for uid in id_list:
            # FIX: validate each ID is actually an integer before fetching
            try:
                user_id = int(uid)
            except ValueError:
                failed += 1
                continue
            try:
                user = await self.bot.fetch_user(user_id)
                await interaction.guild.ban(user, reason=reason)
                banned += 1
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                logger.warning("mass_ban: failed to ban %s — %s", uid, e)
                failed += 1

        await interaction.followup.send(
            f"Mass ban complete. 🔨 I successfully banished {banned} users. {failed} failed (probably invalid IDs or permissions)."
        )

    @app_commands.command(name="kick_member", description="[OWNER] Kick a member from the server.")
    @app_commands.default_permissions(administrator=True)
    async def kick_member(self, interaction: discord.Interaction, target: discord.Member, reason: str = "Removed by Aria."):
        block_reason = self._member_block_reason(interaction, target)
        if block_reason:
            return await interaction.response.send_message(block_reason, ephemeral=True)

        await target.kick(reason=reason)
        await interaction.response.send_message(f"Kicked **{target.display_name}**. The door has been shown to them.")

    @app_commands.command(name="timeout_member", description="[OWNER] Timeout a member for a chosen number of minutes.")
    @app_commands.default_permissions(administrator=True)
    async def timeout_member(self, interaction: discord.Interaction, target: discord.Member, minutes: int, reason: str = "Timed out by Aria."):
        if minutes < 1 or minutes > 40320:
            return await interaction.response.send_message("Timeout minutes must be between 1 and 40320.", ephemeral=True)

        block_reason = self._member_block_reason(interaction, target)
        if block_reason:
            return await interaction.response.send_message(block_reason, ephemeral=True)

        await target.timeout(timedelta(minutes=minutes), reason=reason)
        await interaction.response.send_message(f"Muted **{target.display_name}** for {minutes} minute(s). Blissful quiet.")

    @app_commands.command(name="clear_messages", description="[OWNER] Bulk-delete recent messages from a channel.")
    @app_commands.default_permissions(administrator=True)
    async def clear_messages(self, interaction: discord.Interaction, channel: discord.TextChannel, amount: int, reason: str = "Cleaned by Aria."):
        if amount < 1 or amount > 100:
            return await interaction.response.send_message("Amount must be between 1 and 100 messages.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        deleted = await channel.purge(limit=amount, reason=reason)
        await interaction.followup.send(f"Deleted {len(deleted)} message(s) from {channel.mention}.")

    @app_commands.command(name="set_slowmode", description="[OWNER] Set slowmode for a text channel.")
    @app_commands.default_permissions(administrator=True)
    async def set_slowmode(self, interaction: discord.Interaction, channel: discord.TextChannel, seconds: int, reason: str = "Slowmode adjusted by Aria."):
        if seconds < 0 or seconds > 21600:
            return await interaction.response.send_message("Slowmode must be between 0 and 21600 seconds.", ephemeral=True)

        await channel.edit(slowmode_delay=seconds, reason=reason)
        label = "disabled" if seconds == 0 else f"set to {seconds}s"
        await interaction.response.send_message(f"Slowmode for {channel.mention} is now {label}.")

    @app_commands.command(name="set_member_nick", description="[OWNER] Change or clear a member nickname.")
    @app_commands.default_permissions(administrator=True)
    async def set_member_nick(self, interaction: discord.Interaction, target: discord.Member, nickname: str | None = None, reason: str = "Nickname changed by Aria."):
        block_reason = self._member_block_reason(interaction, target)
        if block_reason:
            return await interaction.response.send_message(block_reason, ephemeral=True)

        await target.edit(nick=nickname, reason=reason)
        await interaction.response.send_message(f"Nickname updated for **{target.display_name}**.")

    @app_commands.command(name="create_role", description="[OWNER] Create a server role with optional color.")
    @app_commands.default_permissions(administrator=True)
    async def create_role(self, interaction: discord.Interaction, name: str, color_hex: str | None = None, hoist: bool = False, mentionable: bool = False):
        try:
            color = self._parse_color(color_hex)
        except ValueError as exc:
            return await interaction.response.send_message(str(exc), ephemeral=True)

        role = await interaction.guild.create_role(
            name=name,
            color=color,
            hoist=hoist,
            mentionable=mentionable,
            reason=f"Role created by {interaction.user}",
        )
        await interaction.response.send_message(f"Created role {role.mention}. Try not to make the hierarchy worse.")

    @app_commands.command(name="delete_role", description="[OWNER] Delete a server role.")
    @app_commands.default_permissions(administrator=True)
    async def delete_role(self, interaction: discord.Interaction, role: discord.Role, reason: str = "Role deleted by Aria."):
        if role >= interaction.guild.me.top_role:
            return await interaction.response.send_message("That role is at or above mine. Move Aria's role higher first.", ephemeral=True)
        if role.is_default():
            return await interaction.response.send_message("I cannot delete @everyone. Obviously.", ephemeral=True)

        role_name = role.name
        await role.delete(reason=reason)
        await interaction.response.send_message(f"Deleted role **{role_name}**.")

    @app_commands.command(name="rename_server", description="[OWNER] Rename the server.")
    @app_commands.default_permissions(administrator=True)
    async def rename_server(self, interaction: discord.Interaction, name: str, reason: str = "Server renamed by Aria."):
        cleaned = name.strip()
        if len(cleaned) < 2 or len(cleaned) > 100:
            return await interaction.response.send_message("Server name must be between 2 and 100 characters.", ephemeral=True)

        old_name = interaction.guild.name
        await interaction.guild.edit(name=cleaned, reason=reason)
        await interaction.response.send_message(f"Server renamed from **{old_name}** to **{cleaned}**.")

    # --- AUTOMATION & STATS ---
    @app_commands.command(name="schedule_message", description="[OWNER] Schedule a message to be sent to a channel later.")
    @app_commands.describe(delay_minutes="How many minutes to wait before sending")
    @app_commands.default_permissions(administrator=True)
    async def schedule_message(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str, delay_minutes: int):
        if delay_minutes < 1:
            return await interaction.response.send_message("Minimum delay is 1 minute. I'm not an instant delivery service.", ephemeral=True)

        await interaction.response.send_message(
            f"Fucking fine. I'll send that to {channel.mention} in {delay_minutes} minutes. I am not your secretary, though.",
            ephemeral=True,
        )

        # FIX: use asyncio.create_task instead of deprecated self.bot.loop.create_task
        asyncio.create_task(self._send_delayed(channel, message, delay_minutes))

    async def _send_delayed(self, channel: discord.TextChannel, message: str, delay_minutes: int):
        await asyncio.sleep(delay_minutes * 60)
        try:
            await channel.send(message)
        except discord.Forbidden:
            logger.warning("schedule_message: no permission to send to channel %s (%s)", channel.name, channel.id)
        except discord.HTTPException as e:
            logger.exception("schedule_message: failed to deliver scheduled message — %s", e)

    @app_commands.command(name="server_stats", description="[OWNER] View high-level statistics for the current server.")
    @app_commands.default_permissions(administrator=True)
    async def server_stats(self, interaction: discord.Interaction):
        guild = interaction.guild
        member_count = guild.member_count
        # FIX: guild.members may be empty if chunk_guilds_at_startup=False and members aren't cached.
        # Use member_count for total and approximate bot count from cache only.
        bot_count = sum(1 for member in guild.members if member.bot)
        human_count = member_count - bot_count

        channel_count = len(guild.channels)
        role_count = len(guild.roles)

        embed = discord.Embed(
            title=f"📊 Statistics for {guild.name}",
            description="Analyzing your pathetic little kingdom.",
            color=discord.Color.dark_purple(),
        )
        embed.add_field(name="Humans", value=str(human_count), inline=True)
        embed.add_field(name="Bots", value=str(bot_count), inline=True)
        embed.add_field(name="Total Channels", value=str(channel_count), inline=True)
        embed.add_field(name="Total Roles", value=str(role_count), inline=True)

        await interaction.response.send_message(embed=embed)


async def setup(bot):
    await bot.add_cog(AdvancedAdmin(bot))
