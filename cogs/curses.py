import discord
from discord.ext import commands
from discord import app_commands
import random
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("discord")

class Curses(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Memory store for active curses: {user_id: {"type": "stalk" | "shush", "expires": datetime}}
        # This stores it in memory so it clears automatically if the bot restarts.
        self.active_curses = {}

    # --- THE BACKGROUND LISTENER ---
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Aria ignores herself and other bots
        if message.author.bot:
            return

        user_id = message.author.id

        # Check if the user currently has an active curse
        if user_id in self.active_curses:
            curse = self.active_curses[user_id]

            # Check if the curse has expired
            if datetime.now() > curse["expires"]:
                del self.active_curses[user_id]
                return

            # Apply "Stalk" Curse (Auto-React)
            if curse["type"] == "stalk":
                emojis = ["🙄", "🗑️", "🤡", "🌮", "🥱"]
                try:
                    await message.add_reaction(random.choice(emojis))
                except discord.Forbidden:
                    pass # Ignore if she lacks reaction permissions in a specific channel
            
            # Apply "Shush" Curse (50% deletion)
            elif curse["type"] == "shush":
                # A 50% chance the message simply vanishes
                if random.random() < 0.50:
                    try:
                        await message.delete()
                    except discord.Forbidden:
                        pass

    # --- THE CURSE COMMAND GROUP ---
    curse_group = app_commands.Group(name="curse", description="[OWNER] Apply or remove Aria's punishments on a member.")

    @curse_group.command(name="stalk", description="Make Aria react to a member's messages for a limited time.")
    @app_commands.describe(minutes="How many minutes the curse lasts (default 10)")
    @app_commands.default_permissions(administrator=True) # Admin only!
    async def curse_stalk(self, interaction: discord.Interaction, target: discord.Member, minutes: int = 10):
        if target == self.bot.user:
            return await interaction.response.send_message("Nice try. I'm not cursing myself.", ephemeral=True)
            
        expires = datetime.now() + timedelta(minutes=minutes)
        self.active_curses[target.id] = {"type": "stalk", "expires": expires}
        
        await interaction.response.send_message(f"Oh, I'm going to enjoy this. Stalking {target.mention} for the next **{minutes} minutes**. Every word they speak will be heavily judged.", ephemeral=False)

    @curse_group.command(name="shush", description="Randomly delete about half of a member's messages for a while.")
    @app_commands.describe(minutes="How many minutes the curse lasts (default 10)")
    @app_commands.default_permissions(administrator=True)
    async def curse_shush(self, interaction: discord.Interaction, target: discord.Member, minutes: int = 10):
        if target == self.bot.user:
            return await interaction.response.send_message("Are you broken? I won't silence myself.", ephemeral=True)
            
        expires = datetime.now() + timedelta(minutes=minutes)
        self.active_curses[target.id] = {"type": "shush", "expires": expires}
        
        await interaction.response.send_message(f"Finally, some peace and quiet. Imposing the 'Shush' protocol on {target.mention} for **{minutes} minutes**. Half of everything they say will vanish into the void.", ephemeral=False)

    @curse_group.command(name="lift", description="Remove every active curse from a member early.")
    @app_commands.default_permissions(administrator=True)
    async def curse_lift(self, interaction: discord.Interaction, target: discord.Member):
        if target.id in self.active_curses:
            del self.active_curses[target.id]
            await interaction.response.send_message(f"Fucking fine. I've lifted the curse on {target.mention}. I was getting bored of them anyway.", ephemeral=False)
        else:
            await interaction.response.send_message(f"{target.display_name} isn't currently cursed. Don't waste my time.", ephemeral=True)

# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Curses(bot))
