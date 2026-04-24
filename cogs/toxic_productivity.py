import discord
from discord.ext import commands
from discord import app_commands
from core.db_helpers import db_cursor
import logging
from datetime import timedelta

logger = logging.getLogger("discord")

class ToxicProductivity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # --- HELPER FUNCTIONS ---
    async def get_balance(self, user_id: int) -> int:
        async with db_cursor() as cur:
            await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
            await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (user_id,))
            res = await cur.fetchone()
            return res[0] if res else 0

    async def update_balance(self, user_id: int, amount: int):
        async with db_cursor() as cur:
            await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (amount, user_id))

    # --- THE TOXIC WORK COMMAND GROUP ---
    prod_group = app_commands.Group(name="toxic_work", description="Weaponized productivity tools for chaos and peer pressure.")

    @prod_group.command(name="sabotage", description="Pay coins to slip a fake task into someone's task list.")
    @app_commands.describe(fake_task="The embarrassing task you want to assign them")
    async def sabotage(self, interaction: discord.Interaction, target: discord.Member, fake_task: str):
        cost = 500
        bal = await self.get_balance(interaction.user.id)
        
        if bal < cost:
            return await interaction.response.send_message(f"Sabotage costs **{cost} coins**. You only have **{bal}**. Stay in your tax bracket.", ephemeral=True)
        
        if target.bot:
            return await interaction.response.send_message("I don't have a task list. I'm a bot. Use your brain.", ephemeral=True)

        # Deduct the coins
        await self.update_balance(interaction.user.id, -cost)
        
        # Inject the task into the target's database
        async with db_cursor() as cur:
            # We assume aria_tasks exists because productivity.py created it!
            await cur.execute("INSERT INTO aria_tasks (user_id, task_name) VALUES (%s, %s)", (target.id, fake_task))
                    
        await interaction.response.send_message(f"Teehee. I've charged you **{cost} coins** and silently added **\"{fake_task}\"** to {target.mention}'s task list. They're going to be so confused.")

    @prod_group.command(name="jail", description="Force a 25-minute focus timeout on yourself or someone else.")
    async def jail(self, interaction: discord.Interaction, target: discord.Member = None):
        user_to_jail = target or interaction.user
        
        # Rule: Only Admins can forcibly jail OTHER people. Normal users can only jail themselves.
        if target and target != interaction.user and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("You don't have permission to forcibly jail other people. Do it to yourself, little bitch.", ephemeral=True)

        try:
            # Time out the user for exactly 25 minutes using Discord's native timeout feature
            await user_to_jail.timeout(timedelta(minutes=25), reason="Pomodoro Focus Jail - Get to work!")
            
            if user_to_jail == interaction.user:
                await interaction.response.send_message(f"🍅 {interaction.user.mention} has voluntarily thrown themselves into **Focus Jail** for 25 minutes. They are now muted from the entire server. See you in half an hour.")
            else:
                await interaction.response.send_message(f"🍅 By Admin decree, {user_to_jail.mention} has been thrown into **Focus Jail** for 25 minutes. They are completely muted. Get to work, you lazy human.")
                
        except discord.Forbidden:
            await interaction.response.send_message(f"I don't have the permissions to mute {user_to_jail.display_name}. Make sure my bot role is sitting higher than their role in the server settings.", ephemeral=True)


# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(ToxicProductivity(bot))
